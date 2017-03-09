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
%%
%% antidote_ccrdt_topk:
%% A computational CRDT that computes a top-K.

-module(antidote_ccrdt_topk).
-behaviour(antidote_ccrdt).
-include("antidote_ccrdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0,
    new/1,
    new/2,
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

-type playerid() :: integer().
-type score() :: integer().
-type pair() :: {playerid(), score()} | {nil, nil}.

-type observable() :: #{integer() => integer()}.
-type size() :: pos_integer().

-type topk() :: {
    observable(),
    pair(),
    size()
}.

-type prepare_update() :: {add, pair()}.
-type effect_update() :: {add, pair()}.

%% Creates a new `topk()` with a size of 100.
-spec new() -> topk().
new() ->
    new(100).

%% Creates a new `topk()` with the given `Size`.
-spec new(pos_integer()) -> topk().
new(Size) when is_integer(Size), Size > 0 ->
    {#{}, {nil, nil}, Size}.

%% Creates a new `topk()` with the given `Topk` and `Size`.
-spec new(observable(), pos_integer()) -> topk().
new(Topk, Size) when is_integer(Size), Size > 0 ->
    {Topk, min(Topk), Size};
new(_, _) ->
    new().

%% Returns the value of the `topk()`.
-spec value(topk()) -> list().
value({Top, _, _}) ->
    maps:to_list(Top).

%% Generates an `effect_update()` from a `prepare_update()`.
%%
%% The supported `prepare_update()` for this data type are:
%% - `{add, pair()}`
-spec downstream(prepare_update(), topk()) -> {ok, effect_update() | noop}.
downstream({add, Elem}, Top) ->
    case changes_state(Elem, Top) of
        true -> {ok, {add, Elem}};
        false -> {ok, noop}
    end.

%% Executes an `effect_update()` operation and returns the resulting state.
%%
%% The executable `effect_update()` for this data type are:
%% - `{add, pair()}`
-spec update(effect_update(), topk()) -> {ok, topk()}.
update({add, {Id, Score}}, TopK) when is_integer(Id), is_integer(Score) ->
    {ok, add(Id, Score, TopK)}.

%% Compares the two given `topk()` states.
-spec equal(topk(), topk()) -> boolean().
equal({Top1, Min1, Size1}, {Top2, Min2, Size2}) ->
    Top1 =:= Top2 andalso Size1 =:= Size2 andalso Min1 =:= Min2.

%% Converts the given `topk()` state into an Erlang `binary()`.
-spec to_binary(topk()) -> binary().
to_binary(TopK) ->
    term_to_binary(TopK).

%% Converts a given Erlang `binary()` into a `topk()`.
-spec from_binary(binary()) -> {ok, topk()}.
from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

%% Checks if the given `prepare_update()` is supported by the `topk()`.
-spec is_operation(any()) -> boolean().
is_operation({add, {Id, Score}}) when is_integer(Id), is_integer(Score) -> true;
is_operation(_) -> false.

%% Checks if the given `effect_update()` is tagged for replication.
-spec is_replicate_tagged(effect_update()) -> boolean().
is_replicate_tagged(_) -> false.

%% Checks if the given `effect_update()` operations can be compacted.
-spec can_compact(effect_update(), effect_update()) -> boolean().
can_compact({add, {Id1, _}}, {add, {Id2, _}}) ->
    Id1 == Id2.

%% Compacts the given `effect_update()` operations.
-spec compact_ops(effect_update(), effect_update()) -> {effect_update(), effect_update()}.
compact_ops({add, {Id1, Score1}}, {add, {Id2, Score2}}) ->
    case Score1 > Score2 of
        true -> {{add, {Id1, Score1}}, {noop}};
        false -> {{noop}, {add, {Id2, Score2}}}
    end.

%% Checks if the data type needs to know its current state to generate
%% `update_effect()` operations.
-spec require_state_downstream(any()) -> boolean().
require_state_downstream(_) -> true.

%%%% Private

%% Attempts to add the `playerid()`, `score()` pair to the `topk()`.
-spec add(playerid(), score(), topk()) -> topk().
add(Id, Score, {Top, {MinId, _} = Min, Size}) ->
    {NewTop, NewMin} = case maps:is_key(Id, Top) of
        true ->
            Old = maps:get(Id, Top),
            T = maps:put(Id, max(Old, Score), Top),
            M = case MinId of
                Id -> min(T);
                _ -> Min
            end,
            {T, M};
        false ->
            Elem = {Id, Score},
            case maps:size(Top) of
                Size ->
                    case cmp(Elem, Min) of
                        true ->
                            T = maps:remove(MinId, Top),
                            T1 = maps:put(Id, Score, T),
                            {T1, min(T1)};
                        false -> {Top, Min}
                    end;
                _ ->
                    T = maps:put(Id, Score, Top),
                    M = case cmp(Min, Elem) orelse Min =:= {nil, nil} of
                        true -> Elem;
                        false -> Min
                    end,
                    {T, M}
            end
    end,
    {NewTop, NewMin, Size}.

%% Checks if attempting to add the given `pair()` to the `topk()` will alter its state.
-spec changes_state(pair(), topk()) -> boolean().
changes_state({Id, Score} = Elem, {Top, Min, Size}) ->
    case maps:size(Top) == Size of
        true ->
            case maps:is_key(Id, Top) of
                true -> Score > maps:get(Id, Top);
                false -> cmp(Elem, Min)
            end;
        false ->
            case maps:is_key(Id, Top) of
                true -> Score > maps:get(Id, Top);
                false -> true
            end
    end.

%% Compares two `pair()`.
-spec cmp(pair(), pair()) -> boolean().
cmp(_, {nil, nil}) -> true;
cmp({Id1, Score1}, {Id2, Score2}) ->
    Score1 > Score2 orelse (Score1 == Score2 andalso Id1 > Id2).

%% Finds the minimum `pair()` in the `topk()` observable state.
-spec min(observable()) -> pair().
min(Top) ->
    List = maps:to_list(Top),
    SortedList = lists:sort(fun(X, Y) -> cmp(Y, X) end, List),
    hd(SortedList).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% Tests the `new/0` function.
new_test() ->
    ?assertEqual({#{}, {nil, nil}, 100}, new()).

%% Tests the `value/1` function.
value_test() ->
    Top = {#{1 => 2, 2 => 2}, {1, 2}, 25},
    ?assertEqual(sets:from_list([{2,2}, {1, 2}]), sets:from_list(value(Top))).

%% Tests several `prepare_update()` operations.
downstream_add_test() ->
    Top = {#{1 => 2, 2 => 2}, {1, 2}, 3},
    {ok, noop} = downstream({add, {1, 1}}, Top),
    {ok, noop} = downstream({add, {1, 2}}, Top),
    {ok, noop} = downstream({add, {1, -1}}, Top),
    {ok, noop} = downstream({add, {2, -1}}, Top),
    {ok, noop} = downstream({add, {2, 1}}, Top),
    {ok, {add, {1, 3}}} = downstream({add, {1, 3}}, Top),
    {ok, {add, {3, 2}}} = downstream({add, {3, 2}}, Top).

%% Tests several `effect_update()` operations.
update_add_test() ->
    Top0 = new(2),
    {ok, Top1} = update({add, {1, 5}}, Top0),
    {ok, Top2} = update({add, {1, 4}}, Top1),
    {ok, Top3} = update({add, {1, 5}}, Top2),
    {ok, Top4} = update({add, {2, 3}}, Top3),
    {ok, Top5} = update({add, {3, 3}}, Top4),
    ?assertEqual([{1, 5}, {3,3}], value(Top5)).

%% Tests the `equal/2` function.
equal_test() ->
    Top1 = {#{1 => 2}, {1, 2}, 5},
    Top2 = {#{1 => 2}, {1, 2}, 25},
    Top3 = {#{1 => 2}, {1, 2}, 25},
    ?assertNot(equal(Top1, Top2)),
    ?assert(equal(Top2, Top3)).

%% Tests the `to_binary/1` and `from_binary/1` functions.
binary_test() ->
    Top1 = {#{1 => 2}, {1, 2}, 5},
    BinaryTop1 = to_binary(Top1),
    {ok, Top2} = from_binary(BinaryTop1),
    ?assert(equal(Top1, Top2)).

-endif.


