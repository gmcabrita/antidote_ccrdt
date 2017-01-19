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

%% antidote_ccrdt_average: A computational CRDT that computes the average

-module(antidote_ccrdt_average).

-behaviour(antidote_ccrdt).

-include("antidote_ccrdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([ new/0,
          new/2,
          value/1,
          downstream/2,
          update/2,
          equal/2,
          to_binary/1,
          from_binary/1,
          is_operation/1,
          can_compact/2,
          compact_ops/2,
          require_state_downstream/1
        ]).

-type average() :: {integer(), integer()}.
-type average_update() :: {add, integer()} | {add, {integer(), integer()}}.
-type average_effect() :: {add, {integer(), integer()}}.

%% @doc Create a new, empty 'average()'
new() ->
    {0, 0}.

%% @doc Create 'average()' with initial values
-spec new(integer(), integer()) -> average().
new(Sum, N) when is_integer(Sum), is_integer(N) ->
    {Sum, N};
new(_, _) ->
    new().

%% @doc The single, total value of an`average()'
-spec value(average()) -> float().
value({Sum, N}) when is_integer(Sum), is_integer(N) ->
    Sum / N.

%% @doc Generate a downstream operation.
%% The first parameter is either the tuple `{add, pos_integer()}`
%% or `{add, {pos_integer(), pos_integer()}}`.
%% The second parameter is the average ccrdt although it isn't used.
-spec downstream(average_update(), any()) -> {ok, average_effect()}.
downstream({add, {Value, N}}, _Average) ->
    {ok, {add, {Value, N}}};
downstream({add, Value}, _Average) ->
    {ok, {add, {Value, 1}}}.

%% @doc Update an `average()'.
%% The first argument is either the tuple `{add, integer()}` or `{add, {integer(), integer()}}`.
%% The 2nd argument is the `average()' to update.
%%
%% returns the updated `average()'
-spec update(average_effect(), average()) -> {ok, average()}.
update({add, {_, 0}}, Average) ->
    {ok, Average};
update({add, {Value, N}}, Average) when is_integer(Value), is_integer(N), N > 0 ->
    {ok, add(Value, N, Average)};
update({add, Value}, Average) when is_integer(Value) ->
    {ok, add(Value, 1, Average)}.

%% @doc Compare if two `average()' are equal. Only returns `true()' if both
%% the sum of values and the number of values are equal.
-spec equal(average(), average()) -> boolean().
equal({Value1, N1}, {Value2, N2}) ->
    Value1 =:= Value2 andalso N1 =:= N2.

-spec to_binary(average()) -> binary().
to_binary(Average) ->
    term_to_binary(Average).

from_binary(Bin) ->
    %% @TODO something smarter
    {ok, binary_to_term(Bin)}.

%% @doc The following operation verifies
%%      that Operation is supported by this particular CCRDT.
-spec is_operation(term()) -> boolean().
is_operation({add, {Value, N}}) when is_integer(Value), is_integer(N) -> true;
is_operation({add, Value}) when is_integer(Value) -> true;
is_operation(_) -> false.

-spec can_compact(average_effect(), average_effect()) -> boolean().
can_compact({add, {_, _}}, {add, {_, _}}) -> true.

-spec compact_ops(average_effect(), average_effect()) -> average_effect().
compact_ops({add, {V1, N1}}, {add, {V2, N2}}) -> {add, {V1 + V2, N1 + N2}}.

%% @doc Returns true if ?MODULE:downstream/2 needs the state of crdt
%%      to generate downstream effect
require_state_downstream(_) ->
    false.


% Priv
-spec add(integer(), pos_integer(), average()) -> average().
add(Value, N, {CurrentValue, CurrentN}) ->
    {CurrentValue + Value, CurrentN + N}.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

new_test() ->
    ?assertEqual({0, 0}, new()).

%% @doc test the correctness of `value()' function
value_test() ->
    Average = {4, 5},
    ?assertEqual(4 / 5, value(Average)).

%% @doc test the correctness of add with only 1 parameter.
update_add_test() ->
    Average0 = new(),
    {ok, Average1} = update({add, 1}, Average0),
    {ok, Average2} = update({add, 2}, Average1),
    {ok, Average3} = update({add, 1}, Average2),
    ?assertEqual(4 / 3, value(Average3)).

%% @doc test the correctness of add using all parameters.
update_add_parameters_test() ->
    Average0 = new(),
    {ok, Average1} = update({add, {7, 2}}, Average0),
    ?assertEqual(7 / 2, value(Average1)).

update_negative_params_test() ->
    Average0 = new(),
    {ok, Average1} = update({add, -7}, Average0),
    {ok, Average2} = update({add, {-5, 5}}, Average1),
    ?assertEqual(-12 / 6, value(Average2)).

equal_test() ->
    Average1 = {4, 1},
    Average2 = {4, 2},
    Average3 = {4, 2},
    ?assertNot(equal(Average1, Average2)),
    ?assert(equal(Average2, Average3)).

binary_test() ->
    Average1 = {4, 1},
    BinaryAverage1 = to_binary(Average1),
    {ok, Average2} = from_binary(BinaryAverage1),
    ?assert(equal(Average1, Average2)).

-endif.


