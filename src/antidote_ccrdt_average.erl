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
%% antidote_ccrdt_average:
%% A computational CRDT that computes the aggregated average.

-module(antidote_ccrdt_average).
-behaviour(antidote_ccrdt).
-include("antidote_ccrdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0,
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

-type sum() :: non_neg_integer().
-type num() :: non_neg_integer().

-type average() :: {sum(), num()}.
-type prepare_update() :: {add, sum()} | {add, average()}.
-type effect_update() :: {add, average()}.

%% Creates a new `average()`.
-spec new() -> average().
new() ->
    {0, 0}.

%% Creates a new `average()` with the given `Sum` and `Num`.
-spec new(sum(), num()) -> average().
new(Sum, Num) when is_integer(Sum), is_integer(Num) ->
    {Sum, Num};
new(_, _) ->
    new().

%% Returns the value of `average()`.
-spec value(average()) -> float().
value({Sum, Num}) when is_integer(Sum), is_integer(Num) ->
    Sum / Num.

%% Generates an `effect_update()` operation from a `prepare_update()`.
%%
%% The supported `prepare_update()` operations for this data type are:
%% - `{add, sum()}`
%% - `{add, average()}`
-spec downstream(prepare_update(), average()) -> {ok, effect_update()}.
downstream({add, {Value, N}}, _) ->
    {ok, {add, {Value, N}}};
downstream({add, Value}, _) ->
    {ok, {add, {Value, 1}}}.

%% Executes an `effect_update()` operation and returns the resulting state.
%%
%% The executable `effect_update()` operations for this data type are:
%% - `{add, sum()}`
%% - `{add, average()}`
-spec update(effect_update(), average()) -> {ok, average()}.
update({add, {_, 0}}, Average) ->
    {ok, Average};
update({add, {Value, N}}, Average) when is_integer(Value), is_integer(N), N > 0 ->
    {ok, add(Value, N, Average)};
update({add, Value}, Average) when is_integer(Value) ->
    {ok, add(Value, 1, Average)}.

%% Compares the two given `average()` states.
-spec equal(average(), average()) -> boolean().
equal({Value1, N1}, {Value2, N2}) ->
    Value1 =:= Value2 andalso N1 =:= N2.

%% Converts the given `average()` state into an Erlang `binary()`.
-spec to_binary(average()) -> binary().
to_binary(Average) ->
    term_to_binary(Average).

%% Converts a given Erlang `binary()` into an `average()`.
-spec from_binary(binary()) -> {ok, average()}.
from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

%% Checks if the given `prepare_update()` is supported by the `average()`.
-spec is_operation(any()) -> boolean().
is_operation({add, {Value, N}}) when is_integer(Value), is_integer(N) -> true;
is_operation({add, Value}) when is_integer(Value) -> true;
is_operation(_) -> false.

%% Checks if the given `effect_update()` is tagged for replication.
-spec is_replicate_tagged(effect_update()) -> boolean().
is_replicate_tagged(_) -> false.

%% Checks if the given `effect_update()` operations can be compacted.
-spec can_compact(effect_update(), effect_update()) -> boolean().
can_compact({add, {_, _}}, {add, {_, _}}) -> true.

%% Compacts the given `effect_update()` operations.
-spec compact_ops(effect_update(), effect_update()) -> {{noop}, effect_update()}.
compact_ops({add, {V1, N1}}, {add, {V2, N2}}) -> {{noop}, {add, {V1 + V2, N1 + N2}}}.

%% Checks if the data type needs to know its current state to generate
%% `update_effect()` operations.
-spec require_state_downstream(any()) -> boolean().
require_state_downstream(_) -> false.

%%%% Private

%% Adds `sum()` and `num()` to the current `average()`.
-spec add(sum(), num(), average()) -> average().
add(Value, N, {CurrentValue, CurrentN}) ->
    {CurrentValue + Value, CurrentN + N}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

%% Tests the `new/0` function.
new_test() ->
    ?assertEqual({0, 0}, new()).

%% Tests the `value/1` function.
value_test() ->
    Average = {4, 5},
    ?assertEqual(4 / 5, value(Average)).

%% Tests the `effect_update()` with only a single parameter.
update_add_test() ->
    Average0 = new(),
    {ok, Average1} = update({add, 1}, Average0),
    {ok, Average2} = update({add, 2}, Average1),
    {ok, Average3} = update({add, 1}, Average2),
    ?assertEqual(4 / 3, value(Average3)).

%% Tests the `effect_update()` with both parameters.
update_add_parameters_test() ->
    Average0 = new(),
    {ok, Average1} = update({add, {7, 2}}, Average0),
    ?assertEqual(7 / 2, value(Average1)).

%% Tests the `effect_update()` with negative parameters.
update_negative_params_test() ->
    Average0 = new(),
    {ok, Average1} = update({add, -7}, Average0),
    {ok, Average2} = update({add, {-5, 5}}, Average1),
    ?assertEqual(-12 / 6, value(Average2)).

%% Tests the `equal/2` function.
equal_test() ->
    Average1 = {4, 1},
    Average2 = {4, 2},
    Average3 = {4, 2},
    ?assertNot(equal(Average1, Average2)),
    ?assert(equal(Average2, Average3)).

%% Tests the `to_binary/1` and `from_binary/1` functions.
binary_test() ->
    Average1 = {4, 1},
    BinaryAverage1 = to_binary(Average1),
    {ok, Average2} = from_binary(BinaryAverage1),
    ?assert(equal(Average1, Average2)).

-endif.


