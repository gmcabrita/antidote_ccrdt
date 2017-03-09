%% -------------------------------------------------------------------
%%
%% Copyright (c) 2017 SyncFree Consortium.  All Rights Reserved.
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
%% antidote_ccrdt_leaderboard:
%% A computational CRDT that computes a top-K with support for permanent player removal.
%% Unlike in the antidote_ccrdt_topkd_rmv data type which supports add-wins semantics,
%% a remove operation in this data type represents a permanent ban from the leaderboard
%% for the given player. With these semantics, the data type does not need to maintain
%% extra metadata for each added element. Furthermore, the leaderboard only needs to
%% keep the highest score for each player.

-module(antidote_ccrdt_leaderboard).
-behaviour(antidote_ccrdt).
-include("antidote_ccrdt.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
    new/0,
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

-type playerid() :: integer().
-type score() :: integer().
-type pair() :: {playerid(), score()} | {nil, nil}.

-type obs_state() :: #{playerid() => score()}.
-type mask_state() :: #{playerid() => score()}.
-type bans() :: sets:set(playerid()).
-type size() :: pos_integer().

-type leaderboard() :: {
    obs_state(),
    mask_state(),
    bans(),
    pair(),
    size()
}.

-type prepare_update() :: {add, pair()} | {ban, playerid()}.
-type effect_update() :: {add, pair()} | {ban, playerid()} | {add_r, pair()}.

%% Creates a new `leaderboard()` with a size of 100.
-spec new() -> leaderboard().
new() ->
    new(100).

%% Creates a new `leaderboard()` with the given `Size`.
-spec new(pos_integer()) -> leaderboard().
new(Size) when is_integer(Size), Size > 0 ->
    {#{}, #{}, sets:new(), {nil, nil}, Size}.

%% Returns the value of the `leaderboard()`.
-spec value(leaderboard()) -> [pair()].
value({Observed, _, _, _, _}) ->
    maps:to_list(Observed).

%% Generates an `effect_update()` from a `prepare_update()`.
%%
%% The supported `prepare_update()` for this data type are:
%% - `{add, {playerid(), score()}}`
%% - `{ban, playerid()}`
-spec downstream(prepare_update(), leaderboard()) -> {ok, effect_update() | noop}.
downstream({add, {Id, Score} = Elem}, {Observed, Masked, Bans, Min, Size}) ->
    case sets:is_element(Id, Bans) of
        true -> {ok, noop};
        false ->
            case {maps:is_key(Id, Observed), Score > maps:get(Id, Observed, -1)} of
                {true, true} -> {ok, {add, Elem}};
                {true, false} -> {ok, noop};
                {false, _} ->
                    case {maps:is_key(Id, Masked), Score > maps:get(Id, Masked, -1)} of
                        {true, false} -> {ok, noop};
                        _ ->
                            case maps:size(Observed) < Size orelse cmp(Elem, Min) of
                                true -> {ok, {add, Elem}};
                                false -> {ok, {add_r, Elem}}
                            end
                    end
            end
    end;
downstream({ban, Id}, {_, _, Bans, _, _}) ->
    case sets:is_element(Id, Bans) of
        true -> {ok, noop};
        false -> {ok, {ban, Id}}
    end.

%% Executes an `effect_update()` operation and returns the resulting state.
%%
%% In the case where a ban removes elements from the observable
%% state of the `leaderboard()` the returning tuple will also contain
%% a list of `effect_update()` that must be propagated to remote replicas.
%%
%% The executable `effect_update()` for this data type are:
%% - `{add, {playerid(), score()}}`
%% - `{add_r, {playerid(), score()}}`
%% - `{ban, playerid()}`
-spec update(effect_update(), leaderboard()) -> {ok, leaderboard()} | {ok, leaderboard(), [effect_update()]}.
update({add_r, {Id, Score}}, Leaderboard) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Leaderboard);
update({add, {Id, Score}}, Leaderboard) when is_integer(Id), is_integer(Score) ->
    add(Id, Score, Leaderboard);
update({ban, Id}, Leaderboard) when is_integer(Id) ->
    ban(Id, Leaderboard).

%% Compares the observable states of the two given `leaderboard()` states.
-spec equal(leaderboard(), leaderboard()) -> boolean().
equal({Observed1, _, _, _, Size1}, {Observed2, _, _, _, Size2}) ->
    Observed1 =:= Observed2 andalso Size1 =:= Size2.

%% Converts the given `leaderboard()` state into an Erlang `binary()`.
-spec to_binary(leaderboard()) -> binary().
to_binary(Leaderboard) ->
    term_to_binary(Leaderboard).

%% Converts a given Erlang `binary()` into a `leaderboard()`.
-spec from_binary(binary()) -> {ok, leaderboard()}.
from_binary(Bin) ->
    {ok, binary_to_term(Bin)}.

%% Checks if the given `prepare_update()` is supported by the `leaderboard()`.
-spec is_operation(any()) -> boolean().
is_operation({add, {Id, Score}}) when is_integer(Id), is_integer(Score) -> true;
is_operation({ban, Id}) when is_integer(Id) -> true;
is_operation(_) -> false.

%% Checks if the given `effect_update()` is tagged for replication.
-spec is_replicate_tagged(effect_update()) -> boolean().
is_replicate_tagged({add_r, _}) -> true;
is_replicate_tagged(_) -> false.

%% Checks if the given `effect_update()` operations can be compacted.
-spec can_compact(effect_update(), effect_update()) -> boolean().
can_compact({add, {Id1, _}}, {add, {Id2, _}}) -> Id1 == Id2;
can_compact({add_r, {Id1, _}}, {add, {Id2, _}}) -> Id1 == Id2;
can_compact({add, {Id1, _}}, {add_r, {Id2, _}}) -> Id1 == Id2;
can_compact({add_r, {Id1, _}}, {add_r, {Id2, _}}) -> Id1 == Id2;

can_compact({add_r, {Id1, _}}, {ban, Id2}) -> Id1 == Id2;
can_compact({add, {Id1, _}}, {ban, Id2}) -> Id1 == Id2;

can_compact({ban, Id1}, {ban, Id2}) -> Id1 == Id2;

can_compact(_, _) -> false.

%% Compacts the given `effect_update()` operations.
-spec compact_ops(effect_update(), effect_update()) -> {effect_update() | {noop}, effect_update() | {noop}}.
compact_ops({add, {_, Score1}} = Op1, {add, {_, Score2}} = Op2) ->
    case Score1 > Score2 of
        true -> {Op1, {noop}};
        false -> {{noop}, Op2}
    end;
compact_ops({add_r, {_, Score1}} = Op1, {add, {_, Score2}} = Op2) ->
    case Score1 > Score2 of
        true -> {Op1, {noop}};
        false -> {{noop}, Op2}
    end;
compact_ops({add, {_, Score1}} = Op1, {add_r, {_, Score2}} = Op2) ->
    case Score1 > Score2 of
        true -> {Op1, {noop}};
        false -> {{noop}, Op2}
    end;
compact_ops({add_r, {_, Score1}} = Op1, {add_r, {_, Score2}} = Op2) ->
    case Score1 > Score2 of
        true -> {Op1, {noop}};
        false -> {{noop}, Op2}
    end;

compact_ops({add_r, _}, {ban, Id2}) ->
    {{noop}, {ban, Id2}};
compact_ops({add, _}, {ban, Id2}) ->
    {{noop}, {ban, Id2}};

compact_ops({ban, _Id1}, {ban, Id2}) ->
    {{noop}, {ban, Id2}}.

%% Checks if the data type needs to know its current state to generate
%% `update_effect()` operations.
-spec require_state_downstream(any()) -> boolean().
require_state_downstream(_) -> true.

%%%% Private

%% Attempts to add the `playerid()`, `score()` pair to the `leaderboard()`.
-spec add(playerid(), score(), leaderboard()) -> {ok, leaderboard()} | {ok, leaderboard(), [effect_update()]}.
add(Id, Score, {Observed, Masked, Bans, {MinId, MinScore} = Min, Size} = Leaderboard) ->
    case sets:is_element(Id, Bans) of
        true -> {ok, Leaderboard};
        false ->
            case maps:is_key(Id, Observed) of
                true ->
                    case Score > maps:get(Id, Observed) of
                        true ->
                            NewObserved = maps:put(Id, Score, Observed),
                            NewMin = case MinId of
                                Id -> min(NewObserved);
                                _ -> Min
                            end,
                            {ok, {NewObserved, Masked, Bans, NewMin, Size}};
                        false -> {ok, Leaderboard}
                    end;
                false ->
                    case maps:size(Observed) == Size of
                        true ->
                            case cmp({Id, Score}, Min) of
                                true ->
                                    Masked1 = maps:remove(Id, Masked),
                                    Observed1 = maps:put(Id, Score, Observed),
                                    NewObserved = maps:remove(MinId, Observed1),
                                    NewMasked = maps:put(MinId, MinScore, Masked1),
                                    NewMin = min(NewObserved),
                                    {ok, {NewObserved, NewMasked, Bans, NewMin, Size}};
                                false ->
                                    case (maps:is_key(Id, Masked) andalso Score > maps:get(Id, Masked))
                                            orelse (not maps:is_key(Id, Masked)) of
                                        true ->
                                            NewMasked = maps:put(Id, Score, Masked),
                                            {ok, {Observed, NewMasked, Bans, Min, Size}};
                                        false -> {ok, Leaderboard}
                                    end
                            end;
                        false ->
                            NewObserved = maps:put(Id, Score, Observed),
                            NewMin = case Min == {nil, nil} orelse cmp(Min, {Id, Score}) of
                                true -> {Id, Score};
                                false -> Min
                            end,
                            {ok, {NewObserved, Masked, Bans, NewMin, Size}}
                    end
            end
    end.

%% Bans `playerid()` from the `leaderboard()`.
-spec ban(playerid(), leaderboard()) -> {ok, leaderboard()} | {ok, leaderboard(), [effect_update()]}.
ban(Id, {Observed, Masked, Bans, {MinId, _ } = Min, Size}) ->
    Masked1 = maps:remove(Id, Masked),
    Observed1 = maps:remove(Id, Observed),
    Bans1 = sets:add_element(Id, Bans),
    case maps:is_key(Id, Observed) of
        true ->
            NewElem = get_largest(Masked),
            case NewElem of
                {nil, nil} ->
                    Min1 = case MinId of
                        Id -> min(Observed1);
                        _ -> Min
                    end,
                    {ok, {Observed1, Masked1, Bans1, Min1, Size}};
                {NewId, NewScore} ->
                    Masked2 = maps:remove(NewId, Masked1),
                    Observed2 = maps:put(NewId, NewScore, Observed1),
                    Min1 = NewElem,
                    {ok, {Observed2, Masked2, Bans1, Min1, Size}, [{add, NewElem}]}
            end;
        false -> {ok, {Observed1, Masked1, Bans1, Min, Size}}
    end.

%% Compares two `pair()`.
-spec cmp(pair(), pair()) -> boolean().
cmp({nil, nil}, _) -> false;
cmp(_, {nil, nil}) -> true;
cmp({Id1, Score1}, {Id2, Score2}) ->
    Score1 > Score2
    orelse (Score1 == Score2 andalso Id1 > Id2).

%% Finds the minimum `pair()` in the `leaderboard()` observable state.
-spec min(obs_state()) -> pair().
min(Observed) ->
    List = maps:to_list(Observed),
    case List of
        [] -> {nil, nil};
        _ -> hd(lists:sort(fun(X, Y) -> cmp(Y, X) end, List))
    end.

%% Finds the maximum `pair()` in the `leaderboard()` masked state.
-spec get_largest(mask_state()) -> pair().
get_largest(Masked) ->
    List = maps:to_list(Masked),
    case List of
        [] -> {nil, nil};
        _ -> hd(lists:sort(fun(X, Y) -> cmp(X, Y) end, List))
    end.

%%%%  EUnit tests

-ifdef(TEST).

%% Tests the `new/0` and `new/1` functions.
create_test() ->
    L1 = new(),
    L2 = new(100),
    ?assertEqual(L1, {#{}, #{}, sets:new(), {nil, nil}, 100}),
    ?assertEqual(L1, L2).

%% Tests the `cmp/2` function.
cmp_test() ->
    ?assertEqual(cmp({nil, nil}, {nil, nil}), false),
    ?assertEqual(cmp({nil, nil}, {1, 2}), false),
    ?assertEqual(cmp({1, 2}, {nil, nil}), true),
    ?assertEqual(cmp({1, 2}, {1, 2}), false),
    ?assertEqual(cmp({1, 2}, {1, 3}), false),
    ?assertEqual(cmp({1, 2}, {2, 2}), false),
    ?assertEqual(cmp({1, 3}, {1, 2}), true),
    ?assertEqual(cmp({2, 2}, {1, 2}), true).

%% Tests the excution of several `prepare_update()` operations and
%% `effect_update()` operations, verifying the `leaderboard()` state
%% between executions.
mixed_test() ->
    Size = 2,
    L = new(Size),

    Id1 = 1,
    Score1 = 2,
    Elem1 = {Id1, Score1},
    Downstream1 = downstream({add, Elem1}, L),
    Op1 = {ok, {add, Elem1}},
    ?assertEqual(Downstream1, Op1),
    {ok, DOp1} = Op1,
    {ok, L1} = update(DOp1, L),
    ?assertEqual(L1, {#{Id1 => Score1},
                      #{},
                      sets:new(),
                      Elem1,
                      Size}),

    Id2 = 2,
    Score2 = 2,
    Downstream2 = downstream({add, {Id2, Score2}}, L1),
    Elem2 = {Id2, Score2},
    Op2 = {ok, {add, Elem2}},
    ?assertEqual(Downstream2, Op2),
    {ok, DOp2} = Op2,
    {ok, L2} = update(DOp2, L1),
    ?assertEqual(L2, {#{Id1 => Score1, Id2 => Score2},
                      #{},
                      sets:new(),
                      Elem1,
                      Size}),

    Id3 = 1,
    Score3 = 0,
    ?assertEqual(downstream({add, {Id3, Score3}}, L2), {ok, noop}),

    Id4 = 42,
    Downstream4 = downstream({ban, Id4}, L2),
    Op4 = {ok, {ban, Id4}},
    ?assertEqual(Downstream4, Op4),
    {ok, DOp4} = Op4,
    {ok, L4} = update(DOp4, L2),
    ?assertEqual(L4, {#{Id1 => Score1, Id2 => Score2},
                      #{},
                      sets:from_list([Id4]),
                      Elem1,
                      Size}),

    Id5 = 100,
    Score5 = 1,
    Downstream5 = downstream({add, {Id5, Score5}}, L4),
    Elem5 = {Id5, Score5},
    Op5 = {ok, {add_r, Elem5}},
    ?assertEqual(Downstream5, Op5),
    {ok, DOp5} = Op5,
    {ok, L5} = update(DOp5, L4),
    ?assertEqual(L5, {#{Id1 => Score1, Id2 => Score2},
                      #{Id5 => Score5},
                      sets:from_list([Id4]),
                      Elem1,
                      Size}),

    Id6 = Id2,
    Downstream6 = downstream({ban, Id6}, L5),
    Op6 = {ok, {ban, Id6}},
    ?assertEqual(Downstream6, Op6),
    {ok, DOp6} = Op6,
    GeneratedDownstreamOp = {add, Elem5},
    {ok, L6, [GeneratedOp]} = update(DOp6, L5),
    ?assertEqual(GeneratedDownstreamOp, GeneratedOp),
    ?assertEqual(L6, {#{Id1 => Score1, Id5 => Score5},
                      #{},
                      sets:from_list([Id4, Id6]),
                      Elem5,
                      Size}),

    BannedElem = {Id4, 50},
    ?assertEqual(downstream({add, BannedElem}, L6), {ok, noop}),
    ?assertEqual(downstream({ban, Id4}, L6), {ok, noop}).

%% Tests adding a `pair()` and then banning the `playerid()`.
ban_after_add_test() ->
    Size = 2,
    L = new(Size),

    Id1 = 1,
    Score1 = 2,
    Elem1 = {Id1, Score1},
    DownstreamAdd = downstream({add, Elem1}, L),
    OpAdd = {ok, {add, Elem1}},
    ?assertEqual(DownstreamAdd, OpAdd),
    {ok, DOpAdd} = OpAdd,
    {ok, L1} = update(DOpAdd, L),
    ?assertEqual(L1, {#{Id1 => Score1},
                      #{},
                      sets:new(),
                      Elem1,
                      Size}),

    DownstreamBan = downstream({ban, Id1}, L1),
    OpBan = {ok, {ban, Id1}},
    ?assertEqual(DownstreamBan, OpBan),
    {ok, DOpBan} = OpBan,
    {ok, L2} = update(DOpBan, L1),
    ?assertEqual(L2, {#{},
                      #{},
                      sets:from_list([Id1]),
                      {nil, nil},
                      Size}).

%% Tests a ban edge case.
ban_test() ->
    Size = 2,
    L = new(Size),

    Id1 = 1,
    Score1 = 2,
    Elem1 = {Id1, Score1},
    DownstreamAdd = downstream({add, Elem1}, L),
    OpAdd = {ok, {add, Elem1}},
    ?assertEqual(DownstreamAdd, OpAdd),
    {ok, DOpAdd} = OpAdd,
    {ok, L1} = update(DOpAdd, L),
    ?assertEqual(L1, {#{Id1 => Score1},
                      #{},
                      sets:new(),
                      Elem1,
                      Size}),

    Id2 = 2,
    Score2 = 1,
    Elem2 = {Id2, Score2},
    DownstreamAdd2 = downstream({add, Elem2}, L1),
    OpAdd2 = {ok, {add, Elem2}},
    ?assertEqual(DownstreamAdd2, OpAdd2),
    {ok, DOpAdd2} = OpAdd2,
    {ok, L2} = update(DOpAdd2, L1),
    ?assertEqual(L2, {#{Id1 => Score1, Id2 => Score2},
                      #{},
                      sets:new(),
                      Elem2,
                      Size}),

    DownstreamBan = downstream({ban, Id1}, L2),
    OpBan = {ok, {ban, Id1}},
    ?assertEqual(DownstreamBan, OpBan),
    {ok, DOpBan} = OpBan,
    {ok, L3} = update(DOpBan, L2),
    ?assertEqual(L3, {#{Id2 => Score2},
                      #{},
                      sets:from_list([Id1]),
                      Elem2,
                      Size}).

%% Tests adding a `pair()` after its `playerid()` has been banned.
add_after_ban_test() ->
    L1 = new(),
    Id = 5,
    {ok, L2} = update({ban, Id}, L1),
    {ok, L3} = update({add, {Id, 30}}, L2),
    ?assertEqual(L2, L3).

%% Tests the execution of `effect_update()` operations which should not modify
%% the state of the `leaderboard()`.
noop_add_test() ->
    L1 = new(1),
    Id = 5,
    {ok, L2} = update({add, {Id, 10}}, L1),
    {ok, L3} = update({add, {Id, 5}}, L2),
    ?assertEqual(L3, L2),

    Id2 = 10,
    {ok, L4} = update({add, {Id2, 9}}, L3),
    {ok, L5} = update({add, {Id2, 6}}, L4),
    ?assertEqual(L4, L5).

%% Tests banning the minimum element, when a replacement for it exists in the `mask_state()`.
ban_min_with_replacement_test() ->
    Size = 2,
    L = new(Size),

    Id1 = 1,
    Score1 = 2,
    Elem1 = {Id1, Score1},
    DownstreamAdd = downstream({add, Elem1}, L),
    OpAdd = {ok, {add, Elem1}},
    ?assertEqual(DownstreamAdd, OpAdd),
    {ok, DOpAdd} = OpAdd,
    {ok, L1} = update(DOpAdd, L),
    ?assertEqual(L1, {#{Id1 => Score1},
                      #{},
                      sets:new(),
                      Elem1,
                      Size}),

    Id2 = 2,
    Score2 = 1,
    Elem2 = {Id2, Score2},
    DownstreamAdd2 = downstream({add, Elem2}, L1),
    OpAdd2 = {ok, {add, Elem2}},
    ?assertEqual(DownstreamAdd2, OpAdd2),
    {ok, DOpAdd2} = OpAdd2,
    {ok, L2} = update(DOpAdd2, L1),
    ?assertEqual(L2, {#{Id1 => Score1, Id2 => Score2},
                      #{},
                      sets:new(),
                      Elem2,
                      Size}),

    Id3 = 3,
    Score3 = 100,
    Elem3 = {Id3, Score3},
    DownstreamAdd3 = downstream({add, Elem3}, L2),
    OpAdd3 = {ok, {add, Elem3}},
    ?assertEqual(DownstreamAdd3, OpAdd3),
    {ok, DOpAdd3} = OpAdd3,
    {ok, L3} = update(DOpAdd3, L2),
    ?assertEqual(L3, {#{Id3 => Score3, Id1 => Score1},
                      #{Id2 => Score2},
                      sets:new(),
                      Elem1,
                      Size}),

    DownstreamBan = downstream({ban, Id1}, L3),
    OpBan = {ok, {ban, Id1}},
    ?assertEqual(DownstreamBan, OpBan),
    {ok, DOpBan} = OpBan,
    {ok, L4, [Generated]} = update(DOpBan, L3),
    ?assertEqual(Generated, {add, Elem2}),
    ?assertEqual(L4, {#{Id3 => Score3, Id2 => Score2},
                      #{},
                      sets:from_list([Id1]),
                      Elem2,
                      Size}).

%% Tests the addition of several `pair()` to trigger edge cases.
add_several_test() ->
    L1 = new(2),
    Elem1 = {5, 50},
    {ok, L2} = update({add, Elem1}, L1),
    ?assertEqual(L2, {#{5 => 50},
                      #{},
                      sets:new(),
                      {5, 50},
                      2}),
    Elem2 = {6, 60},
    Op2 = {add, Elem2},
    {ok, DOp2} = downstream(Op2, L2),
    ?assertEqual(Op2, DOp2),
    {ok, L3} = update(DOp2, L2),
    ?assertEqual(L3, {#{6 => 60, 5 => 50},
                      #{},
                      sets:new(),
                      {5, 50},
                      2}),
    Elem3 = {3, 30},
    Op3 = {add_r, Elem3},
    {ok, DOp3} = downstream({add, Elem3}, L3),
    ?assertEqual(Op3, DOp3),
    {ok, L4} = update(DOp3, L3),
    ?assertEqual(L4, {#{5 => 50, 6 => 60},
                      #{3 => 30},
                      sets:new(),
                      {5, 50},
                      2}),
    Elem4 = {5, 100},
    Op4 = {add, Elem4},
    {ok, DOp4} = downstream(Op4, L4),
    ?assertEqual(Op4, DOp4),
    {ok, L5} = update(DOp4, L4),
    ?assertEqual(L5, {#{5 => 100, 6 => 60},
                      #{3 => 30},
                      sets:new(),
                      {6, 60},
                      2}),
    Elem5 = {3, 40},
    Op5 = {add_r, Elem5},
    {ok, DOp5} = downstream({add, Elem5}, L5),
    ?assertEqual(Op5, DOp5),
    {ok, L6} = update(DOp5, L5),
    ?assertEqual(L6, {#{5 => 100, 6 => 60},
                      #{3 => 40},
                      sets:new(),
                      {6, 60},
                      2}),
    Elem6 = {3, 10},
    Op6 = noop,
    {ok, DOp6} = downstream({add, Elem6}, L6),
    ?assertEqual(Op6, DOp6).

%% Tests the `value/1` function.
value_test() ->
    L1 = new(),
    ?assertEqual(value(L1), []),
    {ok, L2} = update({add, {50, 5}}, L1),
    ?assertEqual(value(L2), [{50, 5}]),
    {ok, L3} = update({add, {45, 6}}, L2),
    ?assertEqual(value(L3), [{45, 6}, {50, 5}]).

%% Tests the `min/1` function.
min_test() ->
    ?assertEqual(min(#{}), {nil, nil}),
    ?assertEqual(min(#{1 => 1}), {1, 1}),
    ?assertEqual(min(#{1 => 1, 2 => 5}), {1, 1}).

%% Tests the `get_largest/1` function.
largest_test() ->
    ?assertEqual(get_largest(#{}), {nil, nil}),
    ?assertEqual(get_largest(#{1 => 1}), {1, 1}),
    ?assertEqual(get_largest(#{1 => 1, 2 => 5}), {2, 5}).

%% Tests the `to_binary/1` and `from_binary/1` functions.
binary_test() ->
    L = new(),
    BinL = to_binary(L),
    {ok, L1} = from_binary(BinL),
    ?assert(equal(L, L1)).

-endif.
