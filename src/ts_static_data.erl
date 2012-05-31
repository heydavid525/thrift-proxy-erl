%%%-------------------------------------------------------------------
%%% File    : ts_static_data.erl
%%% Author  : Dai Wei <dai.wei@openx.com>
%%% Description :
%%%     Read in a file containing Thrift requests and save it to ets 
%%%     table.
%%%
%%% Created :  27 April 2012 by Dai Wei <dai.wei@openx.com>
%%%-------------------------------------------------------------------
-module(ts_static_data).

-include("ts_static_data_types.hrl").
%-include("ts_profile.hrl").

-behaviour(gen_server).

%% API
-export([start_link/2, stop/1, lookup/4]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

%-record(state, {}).


%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------

%% RecFile contains the erlang terms for thrift request.
%% Read RecFile to ets table TableName
start_link(ServerName, RecFile) when is_atom(ServerName) ->
    InitOpts = [RecFile],
    GenServOpts = [],
    gen_server:start_link({local, ServerName}, ?MODULE, InitOpts, 
        GenServOpts).

stop(ServerName) ->
    gen_server:call(ServerName, stop).

% Return the query Thrift arguments.
lookup(ServerName, adtype_fct, AdType, Fun) ->
    gen_server:call(ServerName, {lookup, adtype_fct, AdType, Fun});

% Return the Thrift response (reduce message being passed).
lookup(ServerName, fun_args, Fun, Args) ->
    gen_server:call(ServerName, {lookup, fun_args, Fun, Args}).

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

    Tid = ets:new(dummy_table_name, 
            [set, named_table,
            {keypos, #fun_call.fa}, % Primary key is fct_args pair.
            {read_concurrency, true}]),

    read_to_ets(FileName, Tid),

    lager:info("~p:init is finished.~n", [?MODULE]),
    io:format("init is finished.~n",[]),

    {ok, Tid}.


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

% Return the Thrift response (including {reply, ...}).
handle_call({lookup, fun_args, Fun, TrimmedArgs}, _From, Tid) ->
    LookUpRes = ets:lookup(Tid, 
      #fun_args{fct=Fun, trimmed_args=TrimmedArgs}),
    if 
        LookUpRes =:= [] ->
            lager:warning("No matching key in ets. Key = ~p", 
              [#fun_args{fct=Fun, trimmed_args=TrimmedArgs}]);
        true ->
            ok
    end,
    % Return just the object (not list).
    Res = lists:nth(1, LookUpRes),
    {reply, Res#fun_call.resp, Tid};

% Return the Thrift (untrimmed) argument
handle_call({lookup, adtype_fct, AdType, Fun}, _From, Tid) ->
    LookUpRes = ets:match_object(Tid, 
                  #fun_call{adtype=AdType, fa=#fun_args{fct=Fun, _='_'}, _='_'}),
    if 
        LookUpRes =:= [] ->
            lager:warning("No matching key in ets.");
        true ->
            ok
    end,
    Res = lists:nth(1, LookUpRes),
    {reply, Res#fun_call.full_args, Tid};

handle_call(_Request, _From, Tid) ->
    Reply = ok,
    {reply, Reply, Tid}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
%handle_cast(stop, State) ->
%  {stop, normal, State};
handle_cast(_Msg, Tid) ->
    {noreply, Tid}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, Tid) ->
    {noreply, Tid}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(normal, state) ->
    io:format("~p:terminate~n",[?MODULE]),
    ok.

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, Tid, _Extra) ->
    {ok, Tid}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
read_to_ets(FileName, Tid) ->
    {ok, Terms} = file:consult(FileName),
    read_one(Terms, Tid).

read_one([], _Tid) ->
    ok;
read_one([FirstTerm = #fun_call{} | T], Tid) ->
    ets:insert(Tid, FirstTerm#fun_call{}),
    read_one(T, Tid).

