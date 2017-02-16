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

%% antidote_ccrdt.erl : behaviour for op-based computational CRDTs


-module(antidote_ccrdt).

-include("antidote_ccrdt.hrl").

-define(CCRDTS, [antidote_ccrdt_average,
                antidote_ccrdt_topk,
                antidote_ccrdt_topk_with_deletes
               ]).

-export([is_type/1
        ]).

-callback new() -> ccrdt().
-callback value(ccrdt()) -> value().
-callback downstream(update(), ccrdt()) -> {ok, effect()} | {error, reason()}.
-callback update(effect(), ccrdt()) ->  {ok, ccrdt()} | {ok, ccrdt(), [effect()]}.
-callback require_state_downstream(update()) -> boolean().
-callback is_operation(update()) ->  boolean(). %% Type check
-callback can_compact(effect(), effect()) -> boolean().
-callback compact_ops(effect(), effect()) -> effect().
-callback is_replicate_tagged(effect()) -> boolean().

-callback equal(ccrdt(), ccrdt()) -> boolean().
-callback to_binary(ccrdt()) -> binary().
-callback from_binary(binary()) -> {ok, ccrdt()} | {error, reason()}.

is_type(Type) ->
    is_atom(Type) andalso lists:member(Type, ?CCRDTS).

%% End of Module.
