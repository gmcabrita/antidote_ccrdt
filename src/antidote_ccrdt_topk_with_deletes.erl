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
          require_state_downstream/1
        ]).

-type external_state() :: map().
-type internal_state() :: map().
-type deletes() :: map(). % #{playerid() -> #{actor() -> set of timestamp()}}

-type size() :: integer().
-type playerid() :: integer().
-type score() :: integer().
-type timestamp() :: integer(). %% erlang:monotonic_time()

-type topk_with_deletes_pair() :: {playerid(), score(), timestamp()}.
-type vv() :: map(). % #{actor() -> set of timestamp()}

-type topk_with_deletes() :: {external_state(), internal_state(), deletes(), size()}.
-type topk_with_deletes_update() :: {add, {playerid(), score()}} | {del, {playerid(), actor()}}.
-type topk_with_deletes_effect() :: {add, topk_with_deletes_pair()} | {del, {playerid(), vv()}}.

%% @doc Create a new, empty 'topk_with_deletes()'
-spec new() -> topk_with_deletes().
new() ->
    new(25).

%% @doc Create a new, empty 'topk_with_deletes()'
-spec new(integer()) -> topk_with_deletes().
new(Size) when is_integer(Size), Size > 0 ->
    {#{}, #{}, #{}, Size}.

%% @doc The single, total value of a `topk_with_deletes()'
-spec value(topk_with_deletes()) -> list().
value({External, _, _, _}) ->
    List = maps:to_list(External),
    List1 = lists:sort(fun(X, Y) -> cmp(X,Y) end, List),
    lists:map(fun({Id, Score, _}) -> {Id, Score} end, List1).

%% @doc Generate a downstream operation.
-spec downstream(topk_with_deletes_update(), any()) -> {ok, topk_with_deletes_effect()} | {error, {invalid_id, playerid()}}.
downstream({add, {Id, Score}}, {External, Internal, _, _}) ->
    Ts = erlang:monotonic_time(),
    Elem = {Id, Score, Ts},
    TmpInternal = maps:update_with(Id, fun(Old) -> [Elem | Old] end, [Elem], Internal),
    case External =:= max_k(TmpInternal) of
        true -> {ok, {add, {Id, Score, Ts}}};
        false -> {ok, {add, {Id, Score, Ts}}} %% TODO: how do we make sure this only goes to N nodes??
    end;
downstream({del, {Id, Actor}}, {External, Internal, _, _}) ->
    Ts = maps:get(Id, Internal, invalid_id),
    case Ts == invalid_id of
        true -> {error, {invalid_id, Id}};
        false ->
            Ts1 = lists:map(fun({_,_,T}) -> T end, Ts),
            Ts2 = sets:from_list(Ts1),
            Vv = #{Actor => Ts2},
            Tmp = case maps:get(Id, External) of
                {_, _, T} -> sets:is_element(T, Ts2);
                _ -> false
            end,
            case Tmp of
                true -> {del, {Id, Vv}};
                false -> {del, {Id, Vv}} %% TODO: how do we make sure this only goes to N nodes??
            end
    end.

%% @doc Update a `topk_with_deletes()'.
%% returns the updated `topk_with_deletes()'
-spec update(topk_with_deletes_effect(), topk_with_deletes()) -> {ok, topk_with_deletes()}.
update({add, {Id, Score, Ts}}, TopK) when is_integer(Id), is_integer(Score) ->
    %% TODO: how do we propagate operations from here?
    {ok, add(Id, Score, Ts, TopK)};
update({del, {Id, Vv}}, TopK) when is_integer(Id), is_map(Vv) ->
    %% TODO: how do we propagate operations from here?
    {ok, del(Id, Vv, TopK)}.

%% @doc Compare if two `topk_with_deletes()' are equal. Only returns `true()' if both
%% the top-k contain the same external elements.
-spec equal(topk_with_deletes(), topk_with_deletes()) -> boolean().
equal({External1, _, _, Size1}, {External2, _, _, Size2}) ->
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
is_operation({del, {Id, _Actor}}) when is_integer(Id) -> true;
is_operation(_) -> false.

%% @doc Returns true if ?MODULE:downstream/2 needs the state of crdt
%%      to generate downstream effect
require_state_downstream(_) ->
    true.

% Priv
-spec add(playerid(), score(), timestamp(), topk_with_deletes()) -> topk_with_deletes().
add(_Id, _Score, _Ts, {_External, _Internal, _Deletes, _Size}) ->
    %% TODO: implement
    todo.

-spec del(playerid(), vv(), topk_with_deletes()) -> topk_with_deletes().
del(_Id, _Vv, {_External, _Internal, _Deletes, _Size}) ->
    %% TODO: implement
    todo.

-spec max_k(map()) -> map().
max_k(_Top) ->
    %% TODO: implement
    todo.

-spec cmp(topk_with_deletes_pair(), topk_with_deletes_pair()) -> boolean().
cmp({Id1, Score1, _}, {Id2, Score2, _}) ->
    Score1 > Score2 orelse (Score1 == Score2 andalso Id1 > Id2).

% TODO: delete if not needed
% -spec min(map()) -> boolean().
% min(Top) ->
%     List = maps:to_list(Top),
%     SortedList = lists:sort(fun(X, Y) -> cmp(Y, X) end, List),
%     hd(SortedList).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.