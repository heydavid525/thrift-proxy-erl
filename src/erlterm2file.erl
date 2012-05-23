
%%%-------------------------------------------------------------------
%%% File    : erlterm2file.erl
%%% Author  : Dai Wei <dai.wei@openx.com>
%%% Description : The ErlyBank account server.
%%%
%%% Created :  27 April 2012 by Dai Wei <dai.wei@openx.com>
%%%-------------------------------------------------------------------
-module(erlterm2file).

-behaviour(gen_server).
-include("ts_static_data_types.hrl").

%% API
-export([start_link/2, stop/1, log/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% The state is a single file descriptor.
%-record(state, {}).
%-define(SERVER, ?MODULE).
-define(LOGFILE, "term_log.log").

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
start_link(ServerName, Filename) when is_atom(ServerName) ->
  gen_server:start_link({local, ServerName}, ?MODULE, [Filename], []).

stop(ServerName) ->
  gen_server:call(ServerName, stop).

log(ServerName, Term) ->
  gen_server:call(ServerName, {log, Term}).

% TODO:
get_all_terms(ServerName) ->
  gen_server:call(ServerName, get_all_terms).



%%====================================================================
%% gen_server callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init(FileName) ->
  
  % Need to trigger terminate() to close file.
  process_flag(trap_exit, true),

  case file:open(FileName, [read, append]) of
    {ok, Fd} -> 
      % Append to file by starting at end of file.
      lager:info("Opened ~p", [FileName]),
      %{ok, _Eof} = file:position(Fd, eof),
      %file:position(Fd, bof),
      {ok, Fd};
    {error, Reason} ->
      lager:warning("Can't open ~p.", [FileName]),
      {stop, Reason}
   end.

%%--------------------------------------------------------------------
%% Function: 
%% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
handle_call({log, Term}, _From, Fd) ->
  {reply, io:format(Fd, "~p.~n", [Term]), Fd};

handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
handle_cast(stop, Fd) ->
  {stop, normal, Fd};
handle_cast(_Msg, Fd) ->
  {noreply, Fd}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({'EXIT', Pid, Reason}, Fd) ->
  lager:warning("Received EXIT signal from process ~p with reason ~p." 
    ++ "Terminating.", [Pid, Reason]),
  terminate(normal, Fd);
handle_info(_Info, Fd) ->
  {noreply, Fd}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(normal, Fd) ->
  % check if Fd is open.
  lager:debug("IoDevice ~p is being closed.", [Fd]),
  file:close(Fd);
terminate(shutdown, Fd) ->
  terminate(normal, Fd).

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
%warn(Fmt, As) ->
%  io:format(user, "~p: " ++ Fmt, [?MODULE, As]).

