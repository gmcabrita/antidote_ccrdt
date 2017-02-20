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

-type set() :: gb_sets:set().
-type elems() :: set().
-type removals() :: #{playerid() => vc()}.
-type vc() :: #{dcid() => timestamp()}.

-type state() :: {
    elems(),
    removals(),
    vc(),
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

%% Create a new, empty top-K with default size of 100.
-spec new() -> state().
new() ->
    new(100).

%% Creates an empty top-K with size `Size`.
-spec new(integer()) -> state().
new(Size) when is_integer(Size), Size > 0 ->
    {gb_sets:new(), #{}, #{}, Size}.

%% The observable state of the top-K.
-spec value(state()) -> [{playerid(), score()}].
value({Elems, _, _, Size}) ->
    top_k_values(Elems, Size).

%% Generates a downstream operation.
-spec downstream(prepare(), state()) -> {ok, downstream()}.
downstream({add, {Id, Score}}, S) ->
    Ts = {?DC_META_DATA:get_my_dc_id(), ?TIME:timestamp()},
    Args = {Id, Score, Ts},
    case has_observable_impact({add, Args}, S) of
        true -> {ok, {add, Args}};
        false -> {ok, {add_r, Args}}
    end;
downstream({rmv, Id}, {_, _, Vc, _} = S) ->
    Args = {Id, Vc},
    case has_impact({rmv, Args}, S) of
        {true, _} -> {ok, {rmv, Args}};
        {false, true} -> {ok, {rmv_r, Args}};
        {false, false} -> {ok, noop}
    end.

%% Uses the given operation to update the C-CRDT.
%% In the case where new operations must be propagated after the update a list
%% of `downstream()` is also returned.
-spec update(downstream(), state()) -> {ok, state()} | {ok, state(), [downstream()]}.
update({add_r, {Id, Score, Ts}}, S) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Ts, S);
update({add, {Id, Score, Ts}}, S) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Ts, S);
update({rmv_r, {Id, Vc}}, S) when is_integer(Id), is_map(Vc) ->
    rmv(Id, Vc, S);
update({rmv, {Id, Vc}}, S) when is_integer(Id), is_map(Vc) ->
    rmv(Id, Vc, S).

%% Verifies if two top-K objects are observable equivalent.
-spec equal(state(), state()) -> boolean().
equal({Elems1, _, _, Size1}, {Elems2, _, _, Size2}) ->
    top_k_values(Elems1, Size1) =:= top_k_values(Elems2, Size2).

-spec to_binary(state()) -> binary().
to_binary(S) ->
    term_to_binary(S).

from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

%% Returns true to operations that are supported by the C-CRDT.
-spec is_operation(term()) -> boolean().
is_operation({add, {Id, Score}}) when is_integer(Id), is_integer(Score) ->
    true;
is_operation({rmv, Id}) when is_integer(Id) ->
    true;
is_operation(_) ->
    false.

%% Verifies if the operation is tagged as replicate or not.
%% This is used by the transaction buffer to only send replicate operations
%% to a subset of data centers.
-spec is_replicate_tagged(term()) -> boolean().
is_replicate_tagged({add_r, _}) -> true;
is_replicate_tagged({rmv_r, _}) -> true;
is_replicate_tagged(_) -> false.

%% Verifies if two operations can be compacted.
-spec can_compact(downstream(), downstream()) -> boolean().
can_compact({add, {Id1, _, _}}, {add, {Id2, _, _}}) ->
    Id1 == Id2;
can_compact({add_r, {Id1, _, _}}, {add, {Id2, _, _}}) ->
    Id1 == Id2;

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

%% Compacts two operations together.
-spec compact_ops(downstream(), downstream()) -> {downstream(), downstream()}.
compact_ops({add, {Id1, Score1, {_, Ts1}}}, {add, {Id2, Score2, {_, Ts2}}}) ->
    case Score1 > Score2 orelse Score1 == Score2 andalso Ts1 > Ts2 of
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

%% True if the object requires the current state to generate downstreams.
require_state_downstream(_) -> true.

%%%% Priv

%%
-spec add(playerid(), score(), dcid_timestamp(), state()) -> {ok, state()} |
                                                             {ok, state(), [downstream()]}.
add(Id, Score, {DcId, Timestamp} = Ts, {Elems, Deletes, Vc, Size}) ->
    Vc1 = vc_update(Vc, DcId, Timestamp),
    case deletes_get_timestamp(Deletes, Id, DcId) >= Timestamp of
        true -> % element has been removed already, re-propagate a rmv
            S = {Elems, Deletes, Vc1, Size},
            {ok, S, [{rmv, {Id, deletes_get_vc(Deletes, Id)}}]};
        false ->
            Elems1 = gb_sets:add({Score, Id, Ts}, Elems),
            {ok, {Elems1, Deletes, Vc1, Size}}
    end.

-spec rmv(playerid(), vc(), state()) -> {ok, state()} | {ok, state(), [downstream()]}.
rmv(Id, Vc, {Elems, Deletes, LocalVc, Size}) ->
    Deletes1 = merge_vc(Deletes, Id, Vc),
    TopK = top_k(Elems, Size),
    Elems1 = gb_sets:filter(fun({_, Id1, {DcId, Ts}}) ->
        not (Id == Id1 andalso vc_get_timestamp(Vc, DcId) >= Ts)
    end, Elems),
    TopK1 = top_k(Elems1, Size),
    NewCoreElems = gb_sets:to_list(gb_sets:difference(TopK1, TopK)),
    S = {Elems1, Deletes1, LocalVc, Size},
    case NewCoreElems of
        [] -> {ok, S};
        _ ->
            NewCoreElems1 = lists:map(fun({Score, Id1, Ts}) ->
                {add, {Id1, Score, Ts}}
            end, NewCoreElems),
            {ok, S, NewCoreElems1}
    end.


-spec deletes_get_timestamp(removals(), playerid(), dcid()) -> timestamp().
deletes_get_timestamp(Deletes, Id, DcId) ->
    vc_get_timestamp(deletes_get_vc(Deletes, Id), DcId).

-spec deletes_get_vc(removals(), playerid()) -> vc().
deletes_get_vc(Deletes, Id) ->
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

-spec has_observable_impact(downstream(), state()) -> boolean().
has_observable_impact({add, {Id, Score, Ts}}, {Elems, _, _, Size}) ->
    Elem = {Score, Id, Ts},
    Observable = top_k(gb_sets:add(Elem, Elems), Size),
    gb_sets:is_member(Elem, Observable).

-spec has_impact(downstream(), state()) -> {boolean(), boolean()}.
has_impact({rmv, {Id, VcRmv}}, {Elems, _, _, Size}) ->
    Observable = top_k(Elems, Size),
    List = gb_sets:to_list(Observable),
    ImpactsObservable = reduce_while(
        List,
        false,
        fun(_, Bool) -> not Bool end,
        fun({_, Id1, {DcId, Ts}}, Bool) ->
            Bool orelse
                (Id1 == Id andalso vc_get_timestamp(VcRmv, DcId) >= Ts)
        end
    ),
    case ImpactsObservable of
        true -> {true, true};
        false ->
            Masked = gb_sets:to_list(gb_sets:difference(Elems, Observable)),
            ImpactsMasked = reduce_while(
                Masked,
                false,
                fun(_, Bool) -> not Bool end,
                fun({_, Id1, {DcId, Ts}}, Bool) ->
                    Bool orelse
                        (Id1 == Id andalso vc_get_timestamp(VcRmv, DcId) >= Ts)
                end
            ),
            {false, ImpactsMasked}
    end.

-spec top_k_values(set(), size()) -> [{playerid(), score()}].
top_k_values(Set, Size) ->
    List = lists:reverse(gb_sets:to_list(Set)),
    {Top, _, _} = reduce_while(
        List,
        {[], gb_sets:new(), Size},
        fun(_, {_, _, K}) -> K > 0 end,
        fun({Score, Id, _}, {T, C, K} = Acc) ->
            case gb_sets:is_member(Id, C) of
                true -> Acc;
                false -> {[{Id, Score} | T], gb_sets:add(Id, C), K - 1}
            end
        end),
    Top.

-spec top_k(set(), size()) -> set().
top_k(Set, Size) ->
    List = lists:reverse(gb_sets:to_list(Set)),
    {Top, _, _} = reduce_while(
        List,
        {gb_sets:new(), gb_sets:new(), Size},
        fun(_, {_, _, K}) -> K > 0 end,
        fun({_, Id, _} = E, {T, C, K} = Acc) ->
            case gb_sets:is_member(Id, C) of
                true -> Acc;
                false -> {gb_sets:add(E, T), gb_sets:add(Id, C), K - 1}
            end
        end),
    Top.

reduce_while(Col, Initial, While_Func, Reduce_Func) ->
    try
        lists:foldl(fun (X, Acc) ->
            case While_Func(X, Acc) of
                true -> Reduce_Func(X, Acc);
                false -> throw({halt, Acc})
            end
        end, Initial, Col)
    catch
        throw:{halt, Acc} -> Acc
    end.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

is_equal({Elems1, Deletes1, Vc1, Size1}, {Elems2, Deletes2, Vc2, Size2}) ->
    ?assertEqual(gb_sets:to_list(Elems1), gb_sets:to_list(Elems2)),
    ?assertEqual(Deletes1, Deletes2),
    ?assertEqual(Vc1, Vc2),
    ?assertEqual(Size1, Size2).

%% TODO: simplify tests
mixed_test() ->
    ?DC_META_DATA:start_link(),
    ?TIME:start_link(),
    Size = 2,
    Top = new(Size),
    MyDcId = ?DC_META_DATA:get_my_dc_id(),
    is_equal(Top, {gb_sets:new(), #{}, #{}, Size}),

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
    is_equal(Top1, {gb_sets:from_list([Elem1Internal]),
                    #{},
                    #{MyDcId => Time1},
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
    is_equal(Top2, {gb_sets:from_list([Elem2Internal, Elem1Internal]),
                    #{},
                    #{MyDcId => Time2},
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
    is_equal(Top3, {gb_sets:from_list([Elem3Internal, Elem2Internal, Elem1Internal]),
                    #{},
                    #{MyDcId => Time3},
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
    is_equal(Top4, {gb_sets:from_list([Elem4Internal, Elem3Internal, Elem2Internal, Elem1Internal]),
                    #{},
                    #{MyDcId => Time4},
                    Size}),

    Id5 = 1,
    Downstream5 = downstream({rmv, Id5}, Top4),
    Vc = #{MyDcId => Time4},
    Op5 = {ok, {rmv, {Id5, Vc}}},
    ?assertEqual(Downstream5, Op5),

    {ok, DOp5} = Op5,
    GeneratedDOp4 = {add, Elem4},
    {ok, Top5, [GeneratedDOp4]} = update(DOp5, Top4),
    is_equal(Top5, {gb_sets:from_list([Elem4Internal, Elem2Internal]),
                    #{Id1 => Vc},
                    #{MyDcId => Time4},
                    Size}).

masked_delete_test() ->
    ?DC_META_DATA:start_link(),
    Size = 1,
    Top = new(Size),
    MyDcId = ?DC_META_DATA:get_my_dc_id(),
    {ok, Top1} = update({add, {1, 42, {MyDcId, 1}}}, Top),
    {ok, Top2} = update({add, {2, 5, {MyDcId, 2}}}, Top1),
    {ok, RmvOp} = downstream({rmv, 2}, Top2),
    ?assertEqual(RmvOp, {rmv_r, {2, #{MyDcId => 2}}}),
    {ok, Top3} = update(RmvOp, Top2),
    is_equal(Top3, {gb_sets:from_list([{42, 1, {MyDcId, 1}}]),
                    #{2 => #{MyDcId => 2}},
                    #{MyDcId => 2},
                    1}),
    GeneratedRmvOp = {rmv, element(2, RmvOp)},
    {ok, Top4, [GeneratedRmvOp]} = update({add, {2, 5, {MyDcId, 2}}}, Top3),
    is_equal(Top4, Top3),
    {ok, Top5} = update({rmv, {50, #{MyDcId => 42}}}, Top4),
    is_equal(Top5, {gb_sets:from_list([{42, 1, {MyDcId, 1}}]),
                    #{2 => #{MyDcId => 2}, 50 => #{MyDcId => 42}},
                    #{MyDcId => 2},
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
    ?DC_META_DATA:start_link(),
    Dc1 = ?DC_META_DATA:get_my_dc_id(),
    Dc1Top1 = new(1),
    Dc2Top1 = new(1),
    Id = 1,
    Score1 = 45,
    Score2 = 50,
    {ok, AddOp} = downstream({add, {Id, Score1}}, Dc1Top1),
    ?assertEqual(AddOp, {add, {Id, Score1, {Dc1, ?TIME:get_time()}}}),
    {ok, Dc1Top2} = update(AddOp, Dc1Top1),
    {ok, AddOp2} = downstream({add, {Id, Score2}}, Dc1Top2),
    ?assertEqual(AddOp2, {add, {Id, Score2, {Dc1, ?TIME:get_time()}}}),
    {ok, Dc1Top3} = update(AddOp2, Dc1Top2),
    {ok, Dc2Top2} = update(AddOp2, Dc2Top1),
    {ok, RmvOp} = downstream({rmv, Id}, Dc2Top2),
    {ok, Dc2Top3} = update(RmvOp, Dc2Top2),
    {ok, Dc1Top4} = update(RmvOp, Dc1Top3),
    is_equal(Dc1Top4, {gb_sets:new(),
                       #{Id => #{Dc1 => ?TIME:get_time()}},
                       #{Dc1 => ?TIME:get_time()},
                       1}),
    is_equal(Dc1Top4, Dc2Top3),
    {ok, Dc2Top4, [RmvOp]} = update(AddOp, Dc2Top3),
    is_equal(Dc2Top4, Dc2Top3).

-endif.
