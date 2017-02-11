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

%% antidote_ccrdt_topk_with_deletes: A computational CRDT that computes a topk
%% with support for deleting elements.
%%
%% Elements that were previously added and did not belong in the top-k are
%% maintained in a hidden state. Once some element is removed from the top-k
%% it's place will be filled by some element from the hidden state.

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

-type external_state() :: map().
-type internal_state() :: map().
-type deletes() :: map(). % #{playerid() -> #{dcid() -> timestamp()}}

-type size() :: integer().
-type playerid() :: integer().
-type score() :: integer().
-type timestamp() :: {dcid(), integer()}. %% erlang:timestamp()

-type topk_with_deletes_pair() :: {playerid(), score(), timestamp()}.
-type pair_internal() :: {score(), playerid(), timestamp()}.
-type vv() :: map(). % #{dcid() -> timestamp()}

-type topk_with_deletes() :: {external_state(), internal_state(), deletes(), size()}.
-type topk_with_deletes_update() :: {add, {playerid(), score()}} | {del, playerid()}.
-type topk_with_deletes_effect() :: {add, topk_with_deletes_pair()} |
                                    {del, {playerid(), vv()}} |
                                    {add_r, topk_with_deletes_pair()} |
                                    {del_r, {playerid(), vv()}} | {noop}.

%% @doc Create a new, empty 'topk_with_deletes()'
-spec new() -> topk_with_deletes().
new() ->
    new(100).

%% @doc Create a new, empty 'topk_with_deletes()'
-spec new(integer()) -> topk_with_deletes().
new(Size) when is_integer(Size), Size > 0 ->
    {#{}, #{}, #{}, {nil, nil, nil}, Size}.

%% @doc The single, total value of a `topk_with_deletes()'
-spec value(topk_with_deletes()) -> list().
value({External, _, _, _, _}) ->
    List = maps:values(External),
    List1 = lists:map(fun({Score, Id, _}) -> {Id, Score} end, List),
    lists:sort(fun({Id1, Score1}, {Id2, Score2}) -> cmp({Score1, Id1, nil}, {Score2, Id2, nil}) end, List1).

%% @doc Generate a downstream operation.
-spec downstream(topk_with_deletes_update(), any()) -> {ok, topk_with_deletes_effect()}.
downstream({add, {Id, Score}}, {External, _Internal, _, Min, _Size}) ->
    DcId = ?DC_META_DATA:get_my_dc_id(),
    Ts = {DcId, ?TIME:timestamp()},
    Elem = {Score, Id, Ts},
    ChangesState = case maps:is_key(Id, External) of
        true -> cmp(Elem, maps:get(Id, External));
        false -> cmp(Elem, Min)
    end,
    case ChangesState of
        true -> {ok, {add, {Id, Score, Ts}}};
        false -> {ok, {add_r, {Id, Score, Ts}}}
    end;
downstream({del, Id}, {External, Internal, Deletes, _, _}) ->
    case maps:is_key(Id, Internal) of
        false -> {ok, noop};
        true ->
            Elems = gb_sets:to_list(maps:get(Id, Internal)),
            % grab the known version vector for the given Id (if it exists)
            KnownVv = case maps:is_key(Id, Deletes) of
                true -> maps:get(Id, Deletes);
                false -> #{}
            end,
            % update the version vector
            Vv = lists:foldl(fun({_, _, {DcId, Ts}}, Acc) ->
                Max = case maps:is_key(DcId, Acc) of
                    true -> max_timestamp(maps:get(DcId, Acc), {DcId, Ts});
                    false -> {DcId, Ts}
                end,
                maps:put(DcId, Max, Acc)
            end, KnownVv, Elems),
            Tmp = case maps:is_key(Id, External) of
                true ->
                    ElemTs = element(3, maps:get(Id, External)),
                    vv_contains(Vv, ElemTs);
                false -> false
            end,
            case Tmp of
                true -> {ok, {del, {Id, Vv}}};
                false -> {ok, {del_r, {Id, Vv}}}
            end
    end.

%% @doc Update a `topk_with_deletes()'.
%% returns the updated `topk_with_deletes()'
%%
%% In the case where new operations must be propagated after the update a list
%% of `topk_with_deletes_effect()' is also returned.
-spec update(topk_with_deletes_effect(), topk_with_deletes()) -> {ok, topk_with_deletes()} | {ok, topk_with_deletes(), [topk_with_deletes_effect()]}.
update({add_r, {Id, Score, Ts}}, TopK) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Ts, TopK);
update({add, {Id, Score, Ts}}, TopK) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Ts, TopK);
update({del_r, {Id, Vv}}, TopK) when is_integer(Id), is_map(Vv) ->
    del(Id, Vv, TopK);
update({del, {Id, Vv}}, TopK) when is_integer(Id), is_map(Vv) ->
    del(Id, Vv, TopK).

%% @doc Compare if two `topk_with_deletes()' are equal. Only returns `true()' if both
%% the top-k contain the same external elements.
-spec equal(topk_with_deletes(), topk_with_deletes()) -> boolean().
equal({External1, _, _, _, Size1}, {External2, _, _, _, Size2}) ->
    External1 =:= External2 andalso Size1 =:= Size2.

-spec to_binary(topk_with_deletes()) -> binary().
to_binary(TopK) ->
    term_to_binary(TopK).

from_binary(Bin) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

%% @doc The following operation verifies
%%      that Operation is supported by this particular CCRDT.
-spec is_operation(term()) -> boolean().
is_operation({add, {Id, Score}}) when is_integer(Id), is_integer(Score) -> true;
is_operation({del, Id}) when is_integer(Id) -> true;
is_operation(_) -> false.

%% @doc Verifies if the operation is tagged as replicate or not.
%%      This is used by the transaction buffer to only send replicate operations
%%      to a subset of data centers.
-spec is_replicate_tagged(term()) -> boolean().
is_replicate_tagged({add_r, _}) -> true;
is_replicate_tagged({del_r, _}) -> true;
is_replicate_tagged(_) -> false.

-spec can_compact(topk_with_deletes_effect(), topk_with_deletes_effect()) -> boolean().
can_compact({add, {Id1, _, _}}, {add, {Id2, _, _}}) -> Id1 == Id2;

can_compact({add_r, {Id1, _, Ts}}, {del_r, {Id2, Vv}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);
can_compact({add_r, {Id1, _, Ts}}, {del, {Id2, Vv}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);
can_compact({add, {Id1, _, Ts}}, {del_r, {Id2, Vv}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);
can_compact({add, {Id1, _, Ts}}, {del, {Id2, Vv}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);

can_compact({del_r, {Id1, Vv}}, {add_r, {Id2, _, Ts}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);
can_compact({del_r, {Id1, Vv}}, {add, {Id2, _, Ts}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);
can_compact({del, {Id1, Vv}}, {add_r, {Id2, _, Ts}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);
can_compact({del, {Id1, Vv}}, {add, {Id2, _, Ts}}) -> Id1 == Id2 andalso vv_contains(Vv, Ts);

can_compact({del_r, {Id1, _}}, {del_r, {Id2, _}}) -> Id1 == Id2;
can_compact({del_r, {Id1, _}}, {del, {Id2, _}}) -> Id1 == Id2;
can_compact({del, {Id1, _}}, {del_r, {Id2, _}}) -> Id1 == Id2;
can_compact({del, {Id1, _}}, {del, {Id2, _}}) -> Id1 == Id2;

can_compact(_, _) -> false.

-spec compact_ops(topk_with_deletes_effect(), topk_with_deletes_effect()) -> {topk_with_deletes_effect(), topk_with_deletes_effect()}.
compact_ops({add, {Id1, Score1, Ts1}}, {add, {Id2, Score2, Ts2}}) ->
    case Score1 > Score2 of
        true -> {{add, {Id1, Score1, Ts1}}, {add_r, {Id2, Score2, Ts2}}};
        false -> {{add_r, {Id1, Score1, Ts1}}, {add, {Id2, Score2, Ts2}}}
    end;

compact_ops({add_r, _}, {del_r, {Id2, Vv}}) ->
    {{noop}, {del_r, {Id2, Vv}}};
compact_ops({add_r, _}, {del, {Id2, Vv}}) ->
    {{noop}, {del, {Id2, Vv}}};
compact_ops({add, _}, {del_r, {Id2, Vv}}) ->
    {{noop}, {del_r, {Id2, Vv}}};
compact_ops({add, _}, {del, {Id2, Vv}}) ->
    {{noop}, {del, {Id2, Vv}}};

compact_ops({del_r, {Id1, Vv}}, {add_r, _}) ->
    {{del_r, {Id1, Vv}}, {noop}};
compact_ops({del_r, {Id1, Vv}}, {add, _}) ->
    {{del_r, {Id1, Vv}}, {noop}};
compact_ops({del, {Id1, Vv}}, {add_r, _}) ->
    {{del, {Id1, Vv}}, {noop}};
compact_ops({del, {Id1, Vv}}, {add, _}) ->
    {{del, {Id1, Vv}}, {noop}};

compact_ops({del_r, {_Id1, Vv1}}, {del_r, {Id2, Vv2}}) ->
    {{noop}, {del_r, {Id2, merge_vvs(Vv1, Vv2)}}};
compact_ops({del_r, {_Id1, Vv1}}, {del, {Id2, Vv2}}) ->
    {{noop}, {del, {Id2, merge_vvs(Vv1, Vv2)}}};
compact_ops({del, {_Id1, Vv1}}, {del_r, {Id2, Vv2}}) ->
    {{noop}, {del, {Id2, merge_vvs(Vv1, Vv2)}}};
compact_ops({del, {_Id1, Vv1}}, {del, {Id2, Vv2}}) ->
    {{noop}, {del, {Id2, merge_vvs(Vv1, Vv2)}}}.

%% @doc Returns true if ?MODULE:downstream/2 needs the state of crdt
%%      to generate downstream effect
require_state_downstream(_) ->
    true.

% Priv
-spec add(playerid(), score(), timestamp(), topk_with_deletes()) -> {ok, topk_with_deletes()} | {ok, topk_with_deletes(), [topk_with_deletes_effect()]}.
add(Id, Score, Ts, {External, Internal, Deletes, Min, Size} = Top) ->
    Vv = case maps:is_key(Id, Deletes) of
        true -> maps:get(Id, Deletes);
        false -> #{}
    end,
    case vv_contains(Vv, Ts) of
        true -> {ok, Top, [{del, {Id, Vv}}]};
        false ->
            Elem = {Score, Id, Ts},
            Internal1 =
                case maps:is_key(Id, Internal) of
                    true ->
                        Old = maps:get(Id, Internal),
                        maps:put(Id, gb_sets:add_element(Elem, Old), Internal);
                    false -> maps:put(Id, gb_sets:from_list([Elem]), Internal)
                end,
            {External1, Min1} = recompute_external(External, Min, Size, Id, Elem),
            {ok, {External1, Internal1, Deletes, Min1, Size}}
    end.

-spec del(playerid(), vv(), topk_with_deletes()) -> {ok, topk_with_deletes()} | {ok, topk_with_deletes(), [topk_with_deletes_effect()]}.
del(Id, Vv, {External, Internal, Deletes, Min, Size}) ->
    NewDeletes = merge_vv(Deletes, Id, Vv),
    %% delete stuff from internal
    NewInternal = case maps:is_key(Id, Internal) of
        true ->
            Tmp = maps:get(Id, Internal),
            Tmp1 = gb_sets:filter(fun({_,_,Ts}) -> not vv_contains(Vv, Ts) end, Tmp),
            case gb_sets:size(Tmp1) =:= 0 of
                true -> maps:remove(Id, Internal);
                false -> maps:put(Id, Tmp1, Internal)
            end;
        false -> Internal
    end,
    %% check if external has Id and if said element is contained in the VersionVector
    case maps:is_key(Id, External) andalso vv_contains(Vv, element(3, maps:get(Id, External))) of
        true ->
            TmpExternal = maps:remove(Id, External),
            Values = lists:map(fun(X) ->
                gb_sets:largest(X)
            end, maps:values(NewInternal)),

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
                    {ok, {TmpExternal, NewInternal, NewDeletes, NewMin, Size}};
                false ->
                    NewElem = hd(SortedValues1),
                    {S, I, T} = NewElem,
                    NewExternal = maps:put(I, NewElem, TmpExternal),
                    Top = {NewExternal, NewInternal, NewDeletes, NewElem, Size},
                    {ok, Top, [{add, {I, S, T}}]}
            end;
        false -> {ok, {External, NewInternal, NewDeletes, Min, Size}}
    end.

recompute_external(External, {_, MinId, _} = Min, Size, Id, Elem) ->
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
                    NewMin = case cmp(Min, Elem) orelse Min =:= {nil, nil, nil} of
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

-spec vv_contains(vv(), timestamp()) -> boolean().
vv_contains(Vv, _) when map_size(Vv) == 0 -> false;
vv_contains(Vv, {DcId, Ts1}) ->
    case maps:is_key(DcId, Vv) of
        true ->
            {_, Ts2} = maps:get(DcId, Vv),
            Ts2 >= Ts1;
        false -> false
    end.

-spec merge_vv(deletes(), playerid(), vv()) -> deletes().
merge_vv(Deletes, Id, Vv) ->
    NewVv = case maps:is_key(Id, Deletes) of
        true -> merge_vvs(maps:get(Id, Deletes), Vv);
        false -> Vv
    end,
    maps:put(Id, NewVv, Deletes).

-spec merge_vvs(vv(), vv()) -> vv().
merge_vvs(Vv1, Vv2) ->
    maps:fold(fun(K, Ts, Acc) ->
        Max = case maps:is_key(K, Acc) of
            true -> max_timestamp(Ts, maps:get(K, Acc));
            false -> Ts
        end,
        maps:put(K, Max, Acc)
    end, Vv1, Vv2).


-spec cmp(pair_internal() | nil, pair_internal() | nil) -> boolean().
cmp(nil, _) -> false;
cmp(_, nil) -> true;
cmp({nil, nil, nil}, _) -> false;
cmp(_, {nil, nil, nil}) -> true;
cmp({Score1, Id1, _}, {Score2, Id2, _}) ->
    Score1 > Score2 orelse (Score1 == Score2 andalso Id1 > Id2).

-spec max_timestamp(timestamp(), timestamp()) -> timestamp().
max_timestamp({DcId1, T1}, {DcId2, T2}) ->
    case T1 > T2 of
        true -> {DcId1, T1};
        false -> {DcId2, T2}
    end.

-spec min_external(map()) -> pair_internal() | nil.
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
    ?assertEqual(Top, {#{}, #{}, #{}, {nil, nil, nil}, Size}),

    Id1 = 1,
    Score1 = 2,
    Downstream1 = downstream({add, {Id1, Score1}}, Top),
    Elem1 = {Id1, Score1, {MyDcId, ?TIME:get_time()}},
    Elem1Internal = {Score1, Id1, {MyDcId, ?TIME:get_time()}},
    Op1 = {ok, {add, Elem1}},
    ?assertEqual(Downstream1, Op1),

    {ok, DOp1} = Op1,
    {ok, Top1} = update(DOp1, Top),
    ?assertEqual(Top1, {#{Id1 => Elem1Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal])},
                        #{},
                        Elem1Internal,
                        Size}),

    Id2 = 2,
    Score2 = 2,
    Downstream2 = downstream({add, {Id2, Score2}}, Top1),
    Elem2 = {Id2, Score2, {MyDcId, ?TIME:get_time()}},
    Elem2Internal = {Score2, Id2, {MyDcId, ?TIME:get_time()}},
    Op2 = {ok, {add, Elem2}},
    ?assertEqual(Downstream2, Op2),

    {ok, DOp2} = Op2,
    {ok, Top2} = update(DOp2, Top1),
    ?assertEqual(Top2, {#{Id1 => Elem1Internal, Id2 => Elem2Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal]),
                          Id2 => gb_sets:from_list([Elem2Internal])},
                        #{},
                        Elem1Internal,
                        Size}),

    Id3 = 1,
    Score3 = 0,
    Downstream3 = downstream({add, {Id3, Score3}}, Top2),
    Elem3 = {Id3, Score3, {MyDcId, ?TIME:get_time()}},
    Elem3Internal = {Score3, Id3, {MyDcId, ?TIME:get_time()}},
    Op3 = {ok, {add_r, Elem3}},
    ?assertEqual(Downstream3, Op3),

    {ok, DOp3} = Op3,
    {ok, Top3} = update(DOp3, Top2),
    ?assertEqual(Top3, {#{Id1 => Elem1Internal, Id2 => Elem2Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal, Elem3Internal]),
                          Id2 => gb_sets:from_list([Elem2Internal])},
                        #{},
                        Elem1Internal,
                        Size}),

    NonId = 100,
    ?assertEqual(downstream({del, NonId}, Top3),
                            {ok, noop}),

    Id4 = 100,
    Score4 = 1,
    Downstream4 = downstream({add, {Id4, Score4}}, Top3),
    Elem4 = {Id4, Score4, {MyDcId, ?TIME:get_time()}},
    Elem4Internal = {Score4, Id4, {MyDcId, ?TIME:get_time()}},
    Op4 = {ok, {add_r, Elem4}},
    ?assertEqual(Downstream4, Op4),

    {ok, DOp4} = Op4,
    {ok, Top4} = update(DOp4, Top3),
    ?assertEqual(Top4, {#{Id1 => Elem1Internal, Id2 => Elem2Internal},
                        #{Id1 => gb_sets:from_list([Elem1Internal, Elem3Internal]),
                          Id2 => gb_sets:from_list([Elem2Internal]),
                          Id4 => gb_sets:from_list([Elem4Internal])},
                        #{},
                        Elem1Internal,
                        Size}),

    Id5 = 1,
    Downstream5 = downstream({del, Id5}, Top4),
    Vv = #{MyDcId => max_timestamp(element(3, Elem1), element(3, Elem3))},
    Op5 = {ok, {del, {Id5, Vv}}},
    ?assertEqual(Downstream5, Op5),

    {ok, DOp5} = Op5,
    GeneratedDOp4 = {add, Elem4},
    {ok, Top5, [GeneratedDOp4]} = update(DOp5, Top4),
    ?assertEqual(Top5, {#{Id2 => Elem2Internal, Id4 => Elem4Internal},
                        #{Id2 => gb_sets:from_list([Elem2Internal]),
                          Id4 => gb_sets:from_list([Elem4Internal])},
                        #{Id1 => Vv},
                        Elem4Internal,
                        Size}).

internal_delete_test() ->
    ?TIME:start_link(),
    ?DC_META_DATA:start_link(),
    Size = 1,
    Top = new(Size),
    MyDcId = ?DC_META_DATA:get_my_dc_id(),
    {ok, Top1} = update({add, {1, 42, {MyDcId, 0}}}, Top),
    {ok, Top2} = update({add, {2, 5, {MyDcId, 1}}}, Top1),
    {ok, DelOp} = downstream({del, 2}, Top2),
    ?assertEqual(DelOp, {del_r, {2, #{MyDcId => {MyDcId, 1}}}}),
    {ok, Top3} = update(DelOp, Top2),
    ?assertEqual(Top3, {#{1 => {42, 1, {MyDcId, 0}}},
                        #{1 => gb_sets:from_list([{42, 1, {MyDcId, 0}}])},
                        #{2 => #{MyDcId => {MyDcId, 1}}},
                        {42, 1, {MyDcId, 0}},
                        1}),
    GeneratedDelOp = {del, element(2, DelOp)},
    {ok, Top4, [GeneratedDelOp]} = update({add, {2, 5, {MyDcId, 1}}}, Top3),
    ?assertEqual(Top4, {#{1 => {42, 1, {MyDcId, 0}}},
                        #{1 => gb_sets:from_list([{42, 1, {MyDcId, 0}}])},
                        #{2 => #{MyDcId => {MyDcId, 1}}},
                        {42, 1, {MyDcId, 0}},
                        1}),
    {ok, Top5} = update({del, {50, #{MyDcId => {MyDcId, 42}}}}, Top4),
    ?assertEqual(Top5, {#{1 => {42, 1, {MyDcId, 0}}},
                        #{1 => gb_sets:from_list([{42, 1, {MyDcId, 0}}])},
                        #{2 => #{MyDcId => {MyDcId, 1}},
                          50 => #{MyDcId => {MyDcId, 42}}},
                        {42, 1, {MyDcId, 0}},
                        1}).

vv_contains_test() ->
    ?assertEqual(vv_contains(#{a => {a, 0}}, {a, 1}), false),
    ?assertEqual(vv_contains(#{a => {a, 3}}, {a, 1}), true),
    ?assertEqual(vv_contains(#{a => {a, 3},
                               b => {b, 5}}, {b, 6}), false),
    ?assertEqual(vv_contains(#{a => {a, 3},
                               b => {b, 5}}, {b, 1}), true),
    ?assertEqual(vv_contains(#{a => {a, 3},
                               b => {b, 5}}, {c, 0}), false).

simple_merge_vv_test() ->
    ?assertEqual(merge_vv(#{},
                          1,
                        #{a => {a, 3}}),
                 #{1 => #{a => {a, 3}}}),
    ?assertEqual(merge_vv(#{1 => #{a => {a, 3}}},
                          1,
                          #{a => {a, 3}}),
                 #{1 => #{a => {a, 3}}}),
    ?assertEqual(merge_vv(#{1 => #{a => {a, 3}}},
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
    {ok, DelOp} = downstream({del, Id}, Dc2Top2),
    {ok, Dc2Top3} = update(DelOp, Dc2Top2),
    {ok, Dc1Top4} = update(DelOp, Dc1Top3),
    ?assertEqual(Dc1Top4, {#{}, #{}, #{Id => #{Dc1 => {Dc1, ?TIME:get_time()}}}, {nil, nil, nil}, 1}),
    ?assertEqual(Dc1Top4, Dc2Top3),
    {ok, Dc2Top4, [DelOp]} = update(AddOp, Dc2Top3),
    ?assertEqual(Dc2Top4, Dc2Top3).

-endif.
