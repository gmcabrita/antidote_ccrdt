%% -------------------------------------------------------------------
%%
%% Copyright (c) 2016 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% antidote_ccrdt_topk_rmv:
%% A computational CRDT that computes a topk with support for element removal.

-module(antidote_ccrdt_topk_rmv).

-behaviour(antidote_ccrdt).

-include("antidote_ccrdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TIME, mock_time).
-define(DC_META_DATA, mock_dc_meta_data).
-else.
-define(TIME, erlang).
-define(DC_META_DATA, dc_meta_data_utilities).
-endif.

-export([ new/0,
          new/1,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          is_replicate_tagged/1,
          can_compact/2,
          compact_ops/2,
          require_state_downstream/1
        ]).

-type size() :: pos_integer().
-type playerid() :: integer().
-type score() :: integer().
-type timestamp() :: {integer(), integer(), integer()}.
-type dcid_timestamp() :: {dcid(), timestamp()}.

-type pair() :: {playerid(), score(), dcid_timestamp()}.
-type pair_internal() :: {score(), playerid(), dcid_timestamp()}.

-type obs_state() :: #{playerid() => pair_internal()}.
-type mask_state() :: #{playerid() => gb_sets:set(pair_internal())}.
-type removals() :: #{playerid() => vc()}.
-type vc() :: #{dcid() => timestamp()}.
-type minimum() :: pair_internal() | {nil, nil, nil}.

-type state() :: {
    obs_state(),
    mask_state(),
    removals(),
    vc(),
    minimum(),
    size()
}.

-type prepare() :: {add, {playerid(), score()}} |
                  {rmv, playerid()}.
-type downstream() :: {add, pair()} |
                  {rmv, {playerid(), vc()}} |
                  {add_r, pair()} |
                  {rmv_r, {playerid(), vc()}} |
                  noop |
                  {noop}.

-spec new() -> state().
new() ->
    new(100).

-spec new(integer()) -> state().
new(Size) when is_integer(Size), Size > 0 ->
    {#{}, #{}, #{}, #{}, {nil, nil, nil}, Size}.

-spec value(state()) -> [{playerid(), score()}].
value({External, _, _, _, _, _}) ->
    List = maps:values(External),
    List1 = lists:map(fun({Score, Id, _}) -> {Id, Score} end, List),
    lists:sort(fun({Id1, Score1}, {Id2, Score2}) -> cmp({Score1, Id1, nil}, {Score2, Id2, nil}) end, List1).

-spec downstream(prepare(), state()) -> {ok, downstream()}.
downstream({add, {Id, Score}}, {Observed, _, _, _, Min, _Size}) ->
    Ts = {?DC_META_DATA:get_my_dc_id(), ?TIME:timestamp()},
    Elem = {Id, Score, Ts},
    ElemInternal = {Score, Id, Ts},
    ChangesState = case maps:is_key(Id, Observed) of
        true -> cmp(ElemInternal, maps:get(Id, Observed));
        false -> cmp(ElemInternal, Min)
    end,
    case ChangesState of
        true -> {ok, {add, Elem}};
        false -> {ok, {add_r, Elem}}
    end;
downstream({rmv, Id}, {Observed, Masked, _, Vc, _, _}) ->
    case maps:is_key(Id, Masked) of
        false -> {ok, noop};
        true ->
            case maps:is_key(Id, Observed) of
                true -> {ok, {rmv, {Id, Vc}}};
                false -> {ok, {rmv_r, {Id, Vc}}}
            end
    end.

-spec update(downstream(), state()) -> {ok, state()} | {ok, state(), [downstream()]}.
update({add_r, {Id, Score, Ts}}, TopK) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Ts, TopK);
update({add, {Id, Score, Ts}}, TopK) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Ts, TopK);
update({rmv_r, {Id, Vc}}, TopK) when is_integer(Id), is_map(Vc) ->
    rmv(Id, Vc, TopK);
update({rmv, {Id, Vc}}, TopK) when is_integer(Id), is_map(Vc) ->
    rmv(Id, Vc, TopK).

-spec equal(state(), state()) -> boolean().
equal({Observed1, _, _, _, _, Size1}, {Observed2, _, _, _, _, Size2}) ->
    Observed1 =:= Observed2 andalso Size1 =:= Size2.

-spec to_binary(state()) -> binary().
to_binary(TopK) ->
    term_to_binary(TopK).

from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

-spec is_operation(term()) -> boolean().
is_operation({add, {Id, Score}}) when is_integer(Id), is_integer(Score) -> true;
is_operation({rmv, Id}) when is_integer(Id) -> true;
is_operation(_) -> false.

%% @doc Verifies if the operation is tagged as replicate or not.
%%      This is used by the transaction buffer to only send replicate operations
%%      to a subset of data centers.
-spec is_replicate_tagged(term()) -> boolean().
is_replicate_tagged({add_r, _}) -> true;
is_replicate_tagged({rmv_r, _}) -> true;
is_replicate_tagged(_) -> false.

-spec can_compact(downstream(), downstream()) -> boolean().
can_compact({add, {Id1, _, _}}, {add, {Id2, _, _}}) -> Id1 == Id2;
can_compact({add_r, {Id1, _, _}}, {add, {Id2, _, _}}) -> Id1 == Id2;

can_compact({add_r, {Id1, _, {DcId, Ts}}}, {rmv_r, {Id2, Vc}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({add_r, {Id1, _, {DcId, Ts}}}, {rmv, {Id2, Vc}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({add, {Id1, _, {DcId, Ts}}}, {rmv, {Id2, Vc}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;

can_compact({rmv_r, {Id1, _}}, {rmv_r, {Id2, _}}) -> Id1 == Id2;
can_compact({rmv_r, {Id1, _}}, {rmv, {Id2, _}}) -> Id1 == Id2;
can_compact({rmv, {Id1, _}}, {rmv_r, {Id2, _}}) -> Id1 == Id2;
can_compact({rmv, {Id1, _}}, {rmv, {Id2, _}}) -> Id1 == Id2;

can_compact(_, _) -> false.

-spec compact_ops(downstream(), downstream()) -> {downstream(), downstream()}.
compact_ops({add, {Id1, Score1, Ts1}}, {add, {Id2, Score2, Ts2}}) ->
    case Score1 > Score2 of
        true -> {{add, {Id1, Score1, Ts1}}, {add_r, {Id2, Score2, Ts2}}};
        false -> {{add_r, {Id1, Score1, Ts1}}, {add, {Id2, Score2, Ts2}}}
    end;
compact_ops({add_r, {_, Score1, Ts1}} = Op1, {add, {_, Score2, Ts2}} = Op2) ->
    case Score1 == Score2 andalso Ts1 =:= Ts2 of
        true -> {{noop}, Op2};
        false -> {Op1, Op2}
    end;

compact_ops({add_r, _}, {rmv_r, {Id2, Vc}}) ->
    {{noop}, {rmv_r, {Id2, Vc}}};
compact_ops({add_r, _}, {rmv, {Id2, Vc}}) ->
    {{noop}, {rmv, {Id2, Vc}}};
compact_ops({add, _}, {rmv, {Id2, Vc}}) ->
    {{noop}, {rmv, {Id2, Vc}}};

compact_ops({rmv_r, {_Id1, Vc1}}, {rmv_r, {Id2, Vc2}}) ->
    {{noop}, {rmv_r, {Id2, merge_vcs(Vc1, Vc2)}}};
compact_ops({rmv_r, {_Id1, Vc1}}, {rmv, {Id2, Vc2}}) ->
    {{noop}, {rmv, {Id2, merge_vcs(Vc1, Vc2)}}};
compact_ops({rmv, {_Id1, Vc1}}, {rmv_r, {Id2, Vc2}}) ->
    {{noop}, {rmv, {Id2, merge_vcs(Vc1, Vc2)}}};
compact_ops({rmv, {_Id1, Vc1}}, {rmv, {Id2, Vc2}}) ->
    {{noop}, {rmv, {Id2, merge_vcs(Vc1, Vc2)}}}.

require_state_downstream(_) ->
    true.

% Priv
-spec add(playerid(), score(), dcid_timestamp(), state()) -> {ok, state()} | {ok, state(), [downstream()]}.
add(Id, Score, {DcId, Timestamp} = Ts, {Observed, Masked, Removals, Vc, Min, Size}) ->
    Vc1 = vc_update(Vc, DcId, Timestamp),
    case removals_get_timestamp(Removals, Id, DcId) >= Timestamp of
        true ->
            Top = {Observed, Masked, Removals, Vc1, Min, Size},
            {ok, Top, [{rmv, {Id, removals_get_vc(Removals, Id)}}]};
        false ->
            Elem = {Score, Id, Ts},
            Masked1 =
                case maps:is_key(Id, Masked) of
                    true ->
                        Old = maps:get(Id, Masked),
                        maps:put(Id, gb_sets:add_element(Elem, Old), Masked);
                    false -> maps:put(Id, gb_sets:from_list([Elem]), Masked)
                end,
            {Observed1, Min1} = recompute_observed(Observed, Min, Size, Id, Elem),
            {ok, {Observed1, Masked1, Removals, Vc1, Min1, Size}}
    end.

-spec rmv(playerid(), vc(), state()) -> {ok, state()} | {ok, state(), [downstream()]}.
rmv(Id, VcRmv, {Observed, Masked, Removals, Vc, Min, Size}) ->
    NewRemovals = merge_vc(Removals, Id, VcRmv),
    NewMasked = case maps:is_key(Id, Masked) of
        true ->
            Tmp = maps:get(Id, Masked),
            Tmp1 = gb_sets:filter(fun({_,_,{DcId, Ts}}) ->
                Ts > vc_get_timestamp(VcRmv, DcId)
            end, Tmp),
            case gb_sets:size(Tmp1) =:= 0 of
                true -> maps:remove(Id, Masked);
                false -> maps:put(Id, Tmp1, Masked)
            end;
        false -> Masked
    end,
    ImpactsObserved = case maps:is_key(Id, Observed) of
        true ->
            {_, _, {DcId, Ts}} = maps:get(Id, Observed),
            vc_get_timestamp(VcRmv, DcId) >= Ts;
        false -> false
    end,
    case ImpactsObserved of
        true ->
            TmpObserved = maps:remove(Id, Observed),
            Values = lists:map(fun(X) ->
                gb_sets:largest(X)
            end, maps:values(NewMasked)),

            SortedValues = lists:sort(fun(X, Y) ->
                cmp(X, Y)
            end, Values),

            SortedValues1 = lists:dropwhile(fun({_, I, _}) ->
                maps:is_key(I, TmpObserved)
            end, SortedValues),

            case SortedValues1 =:= [] of
                true ->
                    NewMin = case maps:get(Id, Observed) =:= Min of
                        true -> min_observed(TmpObserved);
                        false -> Min
                    end,
                    {ok, {TmpObserved, NewMasked, NewRemovals, Vc, NewMin, Size}};
                false ->
                    NewElem = hd(SortedValues1),
                    {S, I, T} = NewElem,
                    NewObserved = maps:put(I, NewElem, TmpObserved),
                    Top = {NewObserved, NewMasked, NewRemovals, Vc, min_observed(NewObserved), Size},
                    {ok, Top, [{add, {I, S, T}}]}
            end;
        false -> {ok, {Observed, NewMasked, NewRemovals, Vc, Min, Size}}
    end.

-spec recompute_observed(obs_state(), minimum(), size(), playerid(), pair_internal()) -> {obs_state(), minimum()}.
recompute_observed(Observed, {_, MinId, _} = Min, Size, Id, Elem) ->
    case maps:is_key(Id, Observed) of
        true ->
            Old = maps:get(Id, Observed),
            case cmp(Elem, Old) of
                true ->
                    NewObserved = maps:put(Id, Elem, Observed),
                    NewMin = case Old =:= Min of
                        true -> min_observed(NewObserved);
                        false -> Min
                    end,
                    {NewObserved, NewMin};
                false -> {Observed, Min}
            end;
        false ->
            case maps:size(Observed) < Size of
                true ->
                    NewObserved = maps:put(Id, Elem, Observed),
                    NewMin = case cmp(Min, Elem) orelse Min =:= {nil, nil, nil} of
                        true -> Elem;
                        false -> Min
                    end,
                    {NewObserved, NewMin};
                false ->
                    case cmp(Elem, Min) of
                        true ->
                            TmpObserved = maps:remove(MinId, Observed),
                            NewObserved = maps:put(Id, Elem, TmpObserved),
                            {NewObserved, min_observed(NewObserved)};
                        false -> {Observed, Min}
                    end
            end
    end.

-spec removals_get_timestamp(removals(), playerid(), dcid()) -> timestamp().
removals_get_timestamp(Deletes, Id, DcId) ->
    vc_get_timestamp(removals_get_vc(Deletes, Id), DcId).

-spec removals_get_vc(removals(), playerid()) -> vc().
removals_get_vc(Deletes, Id) ->
    case maps:is_key(Id, Deletes) of
        true -> maps:get(Id, Deletes);
        false -> #{}
    end.

-spec vc_get_timestamp(vc(), dcid()) -> timestamp().
vc_get_timestamp(Vc, DcId) ->
    case maps:is_key(DcId, Vc) of
        true -> maps:get(DcId, Vc);
        false -> 0
    end.

-spec vc_update(vc(), dcid(), timestamp()) -> vc().
vc_update(Vc, DcId, Timestamp) ->
    case maps:is_key(DcId, Vc) of
        true ->
            OldTimestamp = maps:get(DcId, Vc),
            MaxTimestamp = max(Timestamp, OldTimestamp),
            maps:put(DcId, MaxTimestamp, Vc);
        false -> maps:put(DcId, Timestamp, Vc)
    end.

-spec merge_vc(removals(), playerid(), vc()) -> removals().
merge_vc(Deletes, Id, Vc) ->
    NewVc = case maps:is_key(Id, Deletes) of
        true -> merge_vcs(maps:get(Id, Deletes), Vc);
        false -> Vc
    end,
    maps:put(Id, NewVc, Deletes).

-spec merge_vcs(vc(), vc()) -> vc().
merge_vcs(Vc1, Vc2) ->
    maps:fold(fun(K, Ts, Acc) ->
        Max = case maps:is_key(K, Acc) of
            true -> max(Ts, maps:get(K, Acc));
            false -> Ts
        end,
        maps:put(K, Max, Acc)
    end, Vc1, Vc2).


-spec cmp(minimum(), minimum()) -> boolean().
cmp({nil, nil, nil}, _) -> false;
cmp(_, {nil, nil, nil}) -> true;
cmp({Score1, Id1, {_, Ts1}}, {Score2, Id2, {_, Ts2}}) ->
    Score1 > Score2
    orelse (Score1 == Score2 andalso Id1 > Id2)
    orelse (Score1 == Score2 andalso Id1 == Id2 andalso Ts1 > Ts2).

-spec min_observed(obs_state()) -> minimum().
min_observed(Observed) ->
    List = maps:values(Observed),
    SortedList = lists:sort(fun(X, Y) -> cmp(Y, X) end, List),
    case SortedList of
        [] -> {nil, nil, nil};
        _ -> hd(SortedList)
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% TODO: simplify tests
mixed_test() ->
    ?TIME:start_link(),
    ?DC_META_DATA:start_link(),
    Size = 2,
    Top = new(Size),
    MyDcId = ?DC_META_DATA:get_my_dc_id(),
    ?assertEqual(Top, {#{}, #{}, #{}, #{}, {nil, nil, nil}, Size}),

    Id1 = 1,
    Score1 = 2,
    Downstream1 = downstream({add, {Id1, Score1}}, Top),
    Time1 = ?TIME:get_time(),
    Elem1 = {Id1, Score1, {MyDcId, Time1}},
    Elem1Internal = {Score1, Id1, {MyDcId, Time1}},
    Op1 = {ok, {add, Elem1}},
    ?assertEqual(Downstream1, Op1),

    {ok, DOp1} = Op1,
    {ok, Top1} = update(DOp1, Top),
    ?assertEqual(Top1, {#{Id1 => Elem1Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal])},
                        #{},
                        #{MyDcId => Time1},
                        Elem1Internal,
                        Size}),

    Id2 = 2,
    Score2 = 2,
    Downstream2 = downstream({add, {Id2, Score2}}, Top1),
    Time2 = ?TIME:get_time(),
    Elem2 = {Id2, Score2, {MyDcId, Time2}},
    Elem2Internal = {Score2, Id2, {MyDcId, Time2}},
    Op2 = {ok, {add, Elem2}},
    ?assertEqual(Downstream2, Op2),

    {ok, DOp2} = Op2,
    {ok, Top2} = update(DOp2, Top1),
    ?assertEqual(Top2, {#{Id1 => Elem1Internal, Id2 => Elem2Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal]),
                          Id2 => gb_sets:from_list([Elem2Internal])},
                        #{},
                        #{MyDcId => Time2},
                        Elem1Internal,
                        Size}),

    Id3 = 1,
    Score3 = 0,
    Downstream3 = downstream({add, {Id3, Score3}}, Top2),
    Time3 = ?TIME:get_time(),
    Elem3 = {Id3, Score3, {MyDcId, Time3}},
    Elem3Internal = {Score3, Id3, {MyDcId, Time3}},
    Op3 = {ok, {add_r, Elem3}},
    ?assertEqual(Downstream3, Op3),

    {ok, DOp3} = Op3,
    {ok, Top3} = update(DOp3, Top2),
    ?assertEqual(Top3, {#{Id1 => Elem1Internal, Id2 => Elem2Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal, Elem3Internal]),
                          Id2 => gb_sets:from_list([Elem2Internal])},
                        #{},
                        #{MyDcId => Time3},
                        Elem1Internal,
                        Size}),

    NonId = 100,
    ?assertEqual(downstream({rmv, NonId}, Top3),
                            {ok, noop}),

    Id4 = 100,
    Score4 = 1,
    Downstream4 = downstream({add, {Id4, Score4}}, Top3),
    Time4 = ?TIME:get_time(),
    Elem4 = {Id4, Score4, {MyDcId, Time4}},
    Elem4Internal = {Score4, Id4, {MyDcId, Time4}},
    Op4 = {ok, {add_r, Elem4}},
    ?assertEqual(Downstream4, Op4),

    {ok, DOp4} = Op4,
    {ok, Top4} = update(DOp4, Top3),
    ?assertEqual(Top4, {#{Id1 => Elem1Internal, Id2 => Elem2Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal, Elem3Internal]),
                          Id2 => gb_sets:from_list([Elem2Internal]),
                          Id4 => gb_sets:from_list([Elem4Internal])},
                        #{},
                        #{MyDcId => Time4},
                        Elem1Internal,
                        Size}),

    Id5 = 1,
    Downstream5 = downstream({rmv, Id5}, Top4),
    Vc = #{MyDcId => Time4},
    Op5 = {ok, {rmv, {Id5, Vc}}},
    ?assertEqual(Downstream5, Op5),

    {ok, DOp5} = Op5,
    GeneratedDOp4 = {add, Elem4},
    {ok, Top5, [GeneratedDOp4]} = update(DOp5, Top4),
    ?assertEqual(Top5, {#{Id2 => Elem2Internal, Id4 => Elem4Internal},
                        #{Id2 => gb_sets:from_list([Elem2Internal]),
                          Id4 => gb_sets:from_list([Elem4Internal])},
                        #{Id1 => Vc},
                        #{MyDcId => Time4},
                        Elem4Internal,
                        Size}).

masked_delete_test() ->
    ?TIME:start_link(),
    ?DC_META_DATA:start_link(),
    Size = 1,
    Top = new(Size),
    MyDcId = ?DC_META_DATA:get_my_dc_id(),
    {ok, Top1} = update({add, {1, 42, {MyDcId, {0, 0, 1}}}}, Top),
    {ok, Top2} = update({add, {2, 5, {MyDcId, {0, 0, 2}}}}, Top1),
    {ok, RmvOp} = downstream({rmv, 2}, Top2),
    ?assertEqual(RmvOp, {rmv_r, {2, #{MyDcId => {0, 0, 2}}}}),
    {ok, Top3} = update(RmvOp, Top2),
    ?assertEqual(Top3, {#{1 => {42, 1, {MyDcId, {0, 0, 1}}}},
                        #{1 => gb_sets:from_list([{42, 1, {MyDcId, {0, 0, 1}}}])},
                        #{2 => #{MyDcId => {0, 0, 2}}},
                        #{MyDcId => {0, 0, 2}},
                        {42, 1, {MyDcId, {0, 0, 1}}},
                        1}),
    GeneratedRmvOp = {rmv, element(2, RmvOp)},
    {ok, Top4, [GeneratedRmvOp]} = update({add, {2, 5, {MyDcId, {0, 0, 2}}}}, Top3),
    ?assertEqual(Top4, {#{1 => {42, 1, {MyDcId, {0, 0, 1}}}},
                        #{1 => gb_sets:from_list([{42, 1, {MyDcId, {0, 0, 1}}}])},
                        #{2 => #{MyDcId => {0, 0, 2}}},
                        #{MyDcId => {0, 0, 2}},
                        {42, 1, {MyDcId, {0, 0, 1}}},
                        1}),
    {ok, Top5} = update({rmv, {50, #{MyDcId => {0, 0, 42}}}}, Top4),
    ?assertEqual(Top5, {#{1 => {42, 1, {MyDcId, {0, 0, 1}}}},
                        #{1 => gb_sets:from_list([{42, 1, {MyDcId, {0, 0, 1}}}])},
                        #{2 => #{MyDcId => {0, 0, 2}},
                          50 => #{MyDcId => {0, 0, 42}}},
                        #{MyDcId => {0, 0, 2}},
                        {42, 1, {MyDcId, {0, 0, 1}}},
                        1}).

simple_merge_vc_test() ->
    ?assertEqual(merge_vc(#{},
                          1,
                        #{a => {a, 3}}),
                 #{1 => #{a => {a, 3}}}),
    ?assertEqual(merge_vc(#{1 => #{a => {a, 3}}},
                          1,
                          #{a => {a, 3}}),
                 #{1 => #{a => {a, 3}}}),
    ?assertEqual(merge_vc(#{1 => #{a => {a, 3}}},
                          1,
                          #{a => {a, 5}}),
                 #{1 => #{a => {a, 5}}}).

delete_semantics_test() ->
    ?TIME:start_link(),
    ?DC_META_DATA:start_link(),
    Dc1 = ?DC_META_DATA:get_my_dc_id(),
    Dc1Top1 = new(1),
    Dc2Top1 = new(1),
    Id = 1,
    Score1 = 45,
    Score2 = 50,
    {ok, AddOp} = downstream({add, {Id, Score1}}, Dc1Top1),
    {ok, Dc1Top2} = update(AddOp, Dc1Top1),
    {ok, AddOp2} = downstream({add, {Id, Score2}}, Dc1Top1),
    ?assertEqual(AddOp2, {add, {Id, Score2, {Dc1, ?TIME:get_time()}}}),
    {ok, Dc1Top3} = update(AddOp2, Dc1Top2),
    {ok, Dc2Top2} = update(AddOp2, Dc2Top1),
    {ok, DelOp} = downstream({rmv, Id}, Dc2Top2),
    {ok, Dc2Top3} = update(DelOp, Dc2Top2),
    {ok, Dc1Top4} = update(DelOp, Dc1Top3),
    ?assertEqual(Dc1Top4, {#{}, #{}, #{Id => #{Dc1 => ?TIME:get_time()}}, #{Dc1 => ?TIME:get_time()}, {nil, nil, nil}, 1}),
    ?assertEqual(Dc1Top4, Dc2Top3),
    {ok, Dc2Top4, [DelOp]} = update(AddOp, Dc2Top3),
    ?assertEqual(Dc2Top4, Dc2Top3).

-endif.