-module(mock_dc_meta_data).

-behaviour(gen_server).

%% api

-export([start_link/0,
         get_my_dc_id/0,
         set_my_dc_id/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% api

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_my_dc_id() ->
    gen_server:call(?MODULE, get_id).

set_my_dc_id(Id) ->
    gen_server:cast(?MODULE, {set_id, Id}).

init([]) ->
    {ok, replica1}.

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