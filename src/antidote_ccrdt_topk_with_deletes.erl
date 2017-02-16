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

%% antidote_ccrdt_topk_with_deletes:
%% A computational CRDT that computes a topk with support for element removal.
%%
%% Elements that were previously added and did not belong in the top-K are
%% maintained in a masked state. Once some element is removed from the top-K
%% its place will be filled by some element from the masked state.

-module(antidote_ccrdt_topk_with_deletes).

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

-type external_state() :: #{playerid() => topk_with_deletes_pair()}.
-type internal_state() :: #{playerid() => topk_with_deletes_pair()}.
-type deletes() :: #{playerid() => vc()}.

-type size() :: integer().
-type playerid() :: integer().
-type score() :: integer().
-type timestamp() :: integer().
-type dcid_timestamp() :: {dcid(), timestamp()}.

-type topk_with_deletes_pair() :: {playerid(), score(), dcid_timestamp()}.
-type vc() :: #{dcid() => timestamp()}.

-type topk_with_deletes() :: {
    external_state(),
    internal_state(),
    deletes(),
    vc(),
    size()
}.

-type topk_with_deletes_update() :: {add, {playerid(), score()}} |
                                    {del, playerid()}.
-type topk_with_deletes_effect() :: {add, topk_with_deletes_pair()} |
                                    {del, {playerid(), vc()}} |
                                    {add_r, topk_with_deletes_pair()} |
                                    {del_r, {playerid(), vc()}} | {noop}.

%% Create a new, empty `topk_with_deletes()` with default size of 100.
-spec new() -> topk_with_deletes().
new() ->
    new(100).

%% Creates an empty `topk_with_deletes()` with size `Size`.
-spec new(integer()) -> topk_with_deletes().
new(Size) when is_integer(Size), Size > 0 ->
    {#{}, #{}, #{}, #{}, {nil, nil, nil}, Size}.

%% The observable state of `topk_with_deletes()'.
-spec value(topk_with_deletes()) -> list().
value({External, _, _, _, _, _}) ->
    List = maps:values(External),
    List1 = lists:map(fun({Id, Score, _}) -> {Id, Score} end, List),
    lists:sort(fun({Id1, Score1}, {Id2, Score2}) ->
        cmp({Id1, Score1, nil}, {Id2, Score2, nil})
    end, List1).

%% Generates a downstream operation.
-spec downstream(topk_with_deletes_update(),
                 topk_with_deletes()) -> {ok, topk_with_deletes_effect()}.
downstream({add, {Id, Score}}, {External, _, _, _, Min, _}) ->
    DcId = ?DC_META_DATA:get_my_dc_id(),
    Ts = {DcId, ?TIME:timestamp()},
    Elem = {Id, Score, Ts},
    ChangesState = case maps:is_key(Id, External) of
        true -> cmp(Elem, maps:get(Id, External));
        false -> cmp(Elem, Min)
    end,
    case ChangesState of
        true -> {ok, {add, Elem}};
        false -> {ok, {add_r, Elem}}
    end;
downstream({del, Id}, {External, Internal, _Deletes, Vc, _, _}) ->
    case maps:is_key(Id, Internal) of
        false -> {ok, noop};
        true ->
            case maps:is_key(Id, External) of
                true -> {ok, {del, {Id, Vc}}};
                false -> {ok, {del_r, {Id, Vc}}}
            end
    end.

%% Uses the given operation to update the CCRDT.
%% In the case where new operations must be propagated after the update a list
%% of `topk_with_deletes_effect()' is also returned.
-spec update(topk_with_deletes_effect(),
             topk_with_deletes()) -> {ok, topk_with_deletes()} |
                                     {ok, topk_with_deletes(),
                                        [topk_with_deletes_effect()]}.
update({add_r, {Id, Score, Ts}}, TopK) when is_integer(Id),
                                            is_integer(Score) ->
    add(Id, Score, Ts, TopK);
update({add, {Id, Score, Ts}}, TopK) when is_integer(Id),
                                          is_integer(Score) ->
    add(Id, Score, Ts, TopK);
update({del_r, {Id, Vc}}, TopK) when is_integer(Id), is_map(Vc) ->
    del(Id, Vc, TopK);
update({del, {Id, Vc}}, TopK) when is_integer(Id), is_map(Vc) ->
    del(Id, Vc, TopK).

%% Verifies if two `topk_with_deletes()` are observable equivalent.
-spec equal(topk_with_deletes(), topk_with_deletes()) -> boolean().
equal({External1, _, _, _, _, Size1}, {External2, _, _, _, _, Size2}) ->
    External1 =:= External2 andalso Size1 =:= Size2.

-spec to_binary(topk_with_deletes()) -> binary().
to_binary(TopK) ->
    term_to_binary(TopK).

from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

%% The following operation verifies that an operation is supported by this particular CCRDT.
-spec is_operation(term()) -> boolean().
is_operation({add, {Id, Score}}) when is_integer(Id),
                                      is_integer(Score) ->
    true;
is_operation({del, Id}) when is_integer(Id) ->
    true;
is_operation(_) ->
    false.

%% Verifies if the operation is tagged as replicate or not.
%% This is used by the transaction buffer to only send replicate operations
%% to a subset of data centers.
-spec is_replicate_tagged(term()) -> boolean().
is_replicate_tagged({add_r, _}) -> true;
is_replicate_tagged({del_r, _}) -> true;
is_replicate_tagged(_) -> false.

-spec can_compact(topk_with_deletes_effect(), topk_with_deletes_effect()) -> boolean().
can_compact({add, {Id1, _, _}}, {add, {Id2, _, _}}) ->
    Id1 == Id2;

can_compact({add_r, {Id1, _, {DcId, Ts}}}, {del_r, {Id2, Vc}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({add_r, {Id1, _, {DcId, Ts}}}, {del, {Id2, Vc}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({add, {Id1, _, {DcId, Ts}}}, {del_r, {Id2, Vc}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({add, {Id1, _, {DcId, Ts}}}, {del, {Id2, Vc}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;

can_compact({del_r, {Id1, Vc}}, {add_r, {Id2, _, {DcId, Ts}}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({del_r, {Id1, Vc}}, {add, {Id2, _, {DcId, Ts}}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({del, {Id1, Vc}}, {add_r, {Id2, _, {DcId, Ts}}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;
can_compact({del, {Id1, Vc}}, {add, {Id2, _, {DcId, Ts}}}) ->
    Id1 == Id2 andalso vc_get_timestamp(Vc, DcId) >= Ts;

can_compact({del_r, {Id1, _}}, {del_r, {Id2, _}}) -> Id1 == Id2;
can_compact({del_r, {Id1, _}}, {del, {Id2, _}}) -> Id1 == Id2;
can_compact({del, {Id1, _}}, {del_r, {Id2, _}}) -> Id1 == Id2;
can_compact({del, {Id1, _}}, {del, {Id2, _}}) -> Id1 == Id2;

can_compact(_, _) -> false.

-spec compact_ops(topk_with_deletes_effect(), topk_with_deletes_effect()) -> {topk_with_deletes_effect(), topk_with_deletes_effect()}.
compact_ops({add, {Id1, Score1, Ts1}}, {add, {Id2, Score2, Ts2}}) ->
    case Score1 > Score2 of
        true -> {{add, {Id1, Score1, Ts1}}, {noop}};
        false -> {{noop}, {add, {Id2, Score2, Ts2}}}
    end;

compact_ops({add_r, _}, {del_r, {Id2, Vc}}) ->
    {{noop}, {del_r, {Id2, Vc}}};
compact_ops({add_r, _}, {del, {Id2, Vc}}) ->
    {{noop}, {del, {Id2, Vc}}};
compact_ops({add, _}, {del_r, {Id2, Vc}}) ->
    {{noop}, {del_r, {Id2, Vc}}};
compact_ops({add, _}, {del, {Id2, Vc}}) ->
    {{noop}, {del, {Id2, Vc}}};

compact_ops({del_r, {Id1, Vc}}, {add_r, _}) ->
    {{del_r, {Id1, Vc}}, {noop}};
compact_ops({del_r, {Id1, Vc}}, {add, _}) ->
    {{del_r, {Id1, Vc}}, {noop}};
compact_ops({del, {Id1, Vc}}, {add_r, _}) ->
    {{del, {Id1, Vc}}, {noop}};
compact_ops({del, {Id1, Vc}}, {add, _}) ->
    {{del, {Id1, Vc}}, {noop}};

compact_ops({del_r, {_Id1, Vc1}}, {del_r, {Id2, Vc2}}) ->
    {{noop}, {del_r, {Id2, merge_vcs(Vc1, Vc2)}}};
compact_ops({del_r, {_Id1, Vc1}}, {del, {Id2, Vc2}}) ->
    {{noop}, {del, {Id2, merge_vcs(Vc1, Vc2)}}};
compact_ops({del, {_Id1, Vc1}}, {del_r, {Id2, Vc2}}) ->
    {{noop}, {del, {Id2, merge_vcs(Vc1, Vc2)}}};
compact_ops({del, {_Id1, Vc1}}, {del, {Id2, Vc2}}) ->
    {{noop}, {del, {Id2, merge_vcs(Vc1, Vc2)}}}.

%% @doc Returns true if ?MODULE:downstream/2 needs the state of crdt
%%      to generate downstream effect
require_state_downstream(_) ->
    true.

% Priv
-spec add(playerid(), score(), dcid_timestamp(), topk_with_deletes()) -> {ok, topk_with_deletes()} | {ok, topk_with_deletes(), [topk_with_deletes_effect()]}.
add(Id, Score, {ReplicaId, Timestamp} = Ts, {External, Internal, Deletes, Vc, Min, Size} = Top) ->
    case deletes_get_timestamp(Deletes, Id, ReplicaId) >= Timestamp of
        true -> {ok, Top, [{del, {Id, deletes_get_vc(Deletes, Id)}}]};
        false ->
            Elem = {Id, Score, Ts},
            Internal1 =
                case maps:is_key(Id, Internal) of
                    true ->
                        Old = maps:get(Id, Internal),
                        maps:put(Id, max_element(Elem, Old), Internal);
                    false -> maps:put(Id, Elem, Internal)
                end,
            {External1, Min1} = recompute_external(External, Min, Size, Id, Elem),
            Vc1 = vc_update(Vc, ReplicaId, Timestamp),
            {ok, {External1, Internal1, Deletes, Vc1, Min1, Size}}
    end.

-spec del(playerid(), vc(), topk_with_deletes()) -> {ok, topk_with_deletes()} | {ok, topk_with_deletes(), [topk_with_deletes_effect()]}.
del(Id, Vc, {External, Internal, Deletes, LocalVc, Min, Size}) ->
    NewDeletes = merge_vc(Deletes, Id, Vc),
    %% delete stuff from internal
    NewInternal = case maps:is_key(Id, Internal) of
        true ->
            {ElemId, _, {RId, Ts}} = maps:get(Id, Internal),
            case vc_get_timestamp(Vc, RId) >= Ts of
                true -> maps:remove(ElemId, Internal);
                false -> Internal
            end;
        false -> Internal
    end,
    case maps:is_key(Id, External) of
        true ->
            {_, _, {ReplicaId, Timestamp}} = maps:get(Id, External),
            case vc_get_timestamp(Vc, ReplicaId) >= Timestamp of
                true ->
                    TmpExternal = maps:remove(Id, External),
                    Values = maps:values(NewInternal),
                    SortedValues = lists:sort(fun(X, Y) ->
                        cmp(X, Y)
                    end, Values),

                    SortedValues1 = lists:dropwhile(fun({_, I, _}) ->
                        maps:is_key(I, TmpExternal)
                    end, SortedValues),

                    case SortedValues1 =:= [] of
                        true ->
                            NewMin = case maps:get(Id, External) =:= Min of
                                true -> min_external(TmpExternal);
                                false -> Min
                            end,
                            Top = {
                                TmpExternal,
                                NewInternal,
                                NewDeletes,
                                LocalVc,
                                NewMin,
                                Size
                            },
                            {ok, Top};
                        false ->
                            NewElem = hd(SortedValues1),
                            {I, _, _} = NewElem,
                            NewExternal = maps:put(I, NewElem, TmpExternal),
                            Top = {
                                NewExternal,
                                NewInternal,
                                NewDeletes,
                                LocalVc,
                                min_external(NewExternal),
                                Size
                            },
                            {ok, Top, [{add, NewElem}]}
                    end;
                false -> {ok, {External, NewInternal, NewDeletes, LocalVc, Min, Size}}
            end;
        false -> {ok, {External, NewInternal, NewDeletes, LocalVc, Min, Size}}
    end.

-spec recompute_external(external_state(),
                         topk_with_deletes_pair(),
                         size(), playerid(),
                         topk_with_deletes_pair()) ->
                            {external_state(), topk_with_deletes_pair()}.
recompute_external(External, {MinId, _, _} = Min, Size, Id, Elem) ->
    case maps:is_key(Id, External) of
        true ->
            Old = maps:get(Id, External),
            case cmp(Elem, Old) of
                true ->
                    NewExt = maps:put(Id, Elem, External),
                    NewMin = case Old =:= Min of
                        true -> min_external(NewExt);
                        false -> Min
                    end,
                    {NewExt, NewMin};
                false -> {External, Min}
            end;
        false ->
            case maps:size(External) < Size of
                true ->
                    NewExt = maps:put(Id, Elem, External),
                    NewMin = case cmp(Min, Elem)
                                  orelse Min =:= {nil, nil, nil} of
                        true -> Elem;
                        false -> Min
                    end,
                    {NewExt, NewMin};
                false ->
                    case cmp(Elem, Min) of
                        true ->
                            TmpExt = maps:remove(MinId, External),
                            NewExt = maps:put(Id, Elem, TmpExt),
                            {NewExt, min_external(NewExt)};
                        false -> {External, Min}
                    end
            end
    end.


-spec deletes_get_timestamp(deletes(), playerid(), dcid()) -> timestamp().
deletes_get_timestamp(Deletes, Id, ReplicaId) ->
    vc_get_timestamp(deletes_get_vc(Deletes, Id), ReplicaId).

-spec deletes_get_vc(deletes(), playerid()) -> vc().
deletes_get_vc(Deletes, Id) ->
    case maps:is_key(Id, Deletes) of
        true -> maps:get(Id, Deletes);
        false -> #{}
    end.

-spec vc_get_timestamp(vc(), dcid()) -> timestamp().
vc_get_timestamp(Vc, ReplicaId) ->
    case maps:is_key(ReplicaId, Vc) of
        true -> maps:get(ReplicaId, Vc);
        false -> 0
    end.

-spec vc_update(vc(), dcid(), timestamp()) -> vc().
vc_update(Vc, ReplicaId, Timestamp) ->
    case maps:is_key(ReplicaId, Vc) of
        true ->
            OldTimestamp = maps:get(ReplicaId, Vc),
            MaxTimestamp = max_timestamp(Timestamp, OldTimestamp),
            maps:put(ReplicaId, MaxTimestamp, Vc);
        false -> maps:put(ReplicaId, Timestamp, Vc)
    end.

-spec merge_vc(deletes(), playerid(), vc()) -> deletes().
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
            true -> max_timestamp(Ts, maps:get(K, Acc));
            false -> Ts
        end,
        maps:put(K, Max, Acc)
    end, Vc1, Vc2).


-spec cmp(topk_with_deletes_pair() | nil,
          topk_with_deletes_pair() | nil) -> boolean().
cmp(nil, _) -> false;
cmp(_, nil) -> true;
cmp({nil, nil, nil}, _) -> false;
cmp(_, {nil, nil, nil}) -> true;
cmp({Id1, Score1, Ts1}, {Id2, Score2, Ts2}) ->
    Score1 > Score2 orelse
    (Score1 == Score2 andalso Id1 > Id2) orelse
    (Score1 == Score2 andalso Id1 == Id2 andalso Ts1 > Ts2).

-spec max_element(topk_with_deletes_pair(),
                  topk_with_deletes_pair()) -> topk_with_deletes_pair().
max_element(One, Two) ->
    case cmp(One, Two) of
        true -> One;
        false -> Two
    end.

-spec max_timestamp(timestamp(), timestamp()) -> timestamp().
max_timestamp(T1, T2) ->
    case T1 >= T2 of
        true -> T1;
        false -> T2
    end.

-spec min_external(map()) -> topk_with_deletes_pair() | nil.
min_external(External) ->
    List = maps:values(External),
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
    Op1 = {ok, {add, Elem1}},
    ?assertEqual(Downstream1, Op1),

    {ok, DOp1} = Op1,
    {ok, Top1} = update(DOp1, Top),
    ?assertEqual(Top1, {#{Id1 => Elem1},
                        #{Id1 => Elem1},
                        #{},
                        #{MyDcId => Time1},
                        Elem1,
                        Size}),

    Id2 = 2,
    Score2 = 2,
    Downstream2 = downstream({add, {Id2, Score2}}, Top1),
    Time2 = ?TIME:get_time(),
    Elem2 = {Id2, Score2, {MyDcId, Time2}},
    Op2 = {ok, {add, Elem2}},
    ?assertEqual(Downstream2, Op2),

    {ok, DOp2} = Op2,
    {ok, Top2} = update(DOp2, Top1),
    ?assertEqual(Top2, {#{Id1 => Elem1, Id2 => Elem2},
                        #{Id1 => Elem1,
                          Id2 => Elem2},
                        #{},
                        #{MyDcId => Time2},
                        Elem1,
                        Size}),

    Id3 = 1,
    Score3 = 0,
    Downstream3 = downstream({add, {Id3, Score3}}, Top2),
    Time3 = ?TIME:get_time(),
    Elem3 = {Id3, Score3, {MyDcId, Time3}},
    Op3 = {ok, {add_r, Elem3}},
    ?assertEqual(Downstream3, Op3),

    {ok, DOp3} = Op3,
    {ok, Top3} = update(DOp3, Top2),
    ?assertEqual(Top3, {#{Id1 => Elem1, Id2 => Elem2},
                        #{Id1 => Elem1,
                          Id2 => Elem2},
                        #{},
                        #{MyDcId => Time3},
                        Elem1,
                        Size}),

    NonId = 100,
    ?assertEqual(downstream({del, NonId}, Top3),
                            {ok, noop}),

    Id4 = 100,
    Score4 = 1,
    Downstream4 = downstream({add, {Id4, Score4}}, Top3),
    Time4 = ?TIME:get_time(),
    Elem4 = {Id4, Score4, {MyDcId, Time4}},
    Op4 = {ok, {add_r, Elem4}},
    ?assertEqual(Downstream4, Op4),

    {ok, DOp4} = Op4,
    {ok, Top4} = update(DOp4, Top3),
    ?assertEqual(Top4, {#{Id1 => Elem1, Id2 => Elem2},
                        #{Id1 => Elem1,
                          Id2 => Elem2,
                          Id4 => Elem4},
                        #{},
                        #{MyDcId => Time4},
                        Elem1,
                        Size}),

    Id5 = 1,
    Downstream5 = downstream({del, Id5}, Top4),
    Vc = #{MyDcId => Time4},
    Op5 = {ok, {del, {Id5, Vc}}},
    ?assertEqual(Downstream5, Op5),

    {ok, DOp5} = Op5,
    GeneratedDOp4 = {add, Elem4},
    {ok, Top5, [GeneratedDOp4]} = update(DOp5, Top4),
    ?assertEqual(Top5, {#{Id2 => Elem2, Id4 => Elem4},
                        #{Id2 => Elem2,
                          Id4 => Elem4},
                        #{Id1 => Vc},
                        #{MyDcId => Time4},
                        Elem4,
                        Size}).

internal_delete_test() ->
    ?TIME:start_link(),
    ?DC_META_DATA:start_link(),
    Size = 1,
    Top = new(Size),
    MyDcId = ?DC_META_DATA:get_my_dc_id(),
    {ok, Top1} = update({add, {1, 42, {MyDcId, 1}}}, Top),
    {ok, Top2} = update({add, {2, 5, {MyDcId, 2}}}, Top1),
    {ok, DelOp} = downstream({del, 2}, Top2),
    ?assertEqual(DelOp, {del_r, {2, #{MyDcId => 2}}}),
    {ok, Top3} = update(DelOp, Top2),
    ?assertEqual(Top3, {#{1 => {1, 42, {MyDcId, 1}}},
                        #{1 => {1, 42, {MyDcId, 1}}},
                        #{2 => #{MyDcId => 2}},
                        #{MyDcId => 2},
                        {1, 42, {MyDcId, 1}},
                        1}),
    GeneratedDelOp = {del, element(2, DelOp)},
    {ok, Top4, [GeneratedDelOp]} = update({add, {2, 5, {MyDcId, 2}}}, Top3),
    ?assertEqual(Top4, Top3),
    {ok, Top5} = update({del, {50, #{MyDcId => 42}}}, Top4),
    ?assertEqual(Top5, {#{1 => {1, 42, {MyDcId, 1}}},
                        #{1 => {1, 42, {MyDcId, 1}}},
                        #{2 => #{MyDcId => 2},
                          50 => #{MyDcId => 42}},
                        #{MyDcId => 2},
                        {1, 42, {MyDcId, 1}},
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
    Time1 = ?TIME:get_time(),
    ?assertEqual(AddOp, {add, {Id, Score1, {Dc1, Time1}}}),
    {ok, Dc1Top2} = update(AddOp, Dc1Top1),
    {ok, AddOp2} = downstream({add, {Id, Score2}}, Dc1Top2),
    Time2 = ?TIME:get_time(),
    ?assertEqual(AddOp2, {add, {Id, Score2, {Dc1, Time2}}}),
    {ok, Dc1Top3} = update(AddOp2, Dc1Top2),
    {ok, Dc2Top2} = update(AddOp2, Dc2Top1),
    {ok, DelOp} = downstream({del, Id}, Dc2Top2),
    {ok, Dc2Top3} = update(DelOp, Dc2Top2),
    {ok, Dc1Top4} = update(DelOp, Dc1Top3),
    ?assertEqual(Dc1Top4, {#{}, #{}, #{Id => #{Dc1 => Time2}}, #{Dc1 => Time2}, {nil, nil, nil}, 1}),
    ?assertEqual(Dc1Top4, Dc2Top3),
    {ok, Dc2Top4, [DelOp]} = update(AddOp, Dc2Top3),
    ?assertEqual(Dc2Top4, Dc2Top3).

-endif.
