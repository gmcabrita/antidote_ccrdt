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
%% mock_dc_meta_data:
%% This module is used for mocking Antidote's dc_meta_data module and
%% is used in certain computational CRDT tests.

-module(mock_dc_meta_data).
-behaviour(gen_server).

-export([
    start_link/0,
    get_my_dc_id/0,
    set_my_dc_id/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%%%% API

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_my_dc_id() ->
    gen_server:call(?MODULE, get_id).

set_my_dc_id(Id) ->
    gen_server:cast(?MODULE, {set_id, Id}).

init([]) ->
    {ok, {replica1, 0}}.

%% gen_server callbacks

handle_call(get_id, _From, State) ->
    {reply, State, State}.

handle_cast({set_id, Id}, _) ->
    {noreply, Id}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
