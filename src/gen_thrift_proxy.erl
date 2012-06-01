%%%-------------------------------------------------------------------
%%% File    : gen_thrift_proxy.erl
%%% Author  : Dai Wei <dai.wei@openx.com>
%%% Description : 
%%%   The proxy server to be 'subclassed' by other proxies.
%%%
%%% Created :  17 May 2012 by Dai Wei <dai.wei@openx.com>
%%%-------------------------------------------------------------------
-module(gen_thrift_proxy).

-behaviour(gen_server).

-include("ts_static_data_types.hrl").

%% API
-export([start_link/6, 
         stop/1,
         handle_function/3,
         handle_function_cast/3,
         set_adtype/2,
         get_adtype/1,
         make_request_cast/3,
         make_request_call/3]).

%% Utility API
-export([trim_args/3,
         trim_args_helper/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, 
        {proxy_name,
         proxy_client_name,
         thrift_svc,
         server_port,
         client_port,
         log_server,
         mode,           % mode = {proxy, replay_reply, replay_request} 
         adtype
        }).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
% Proxy needs to be the module name.
start_link(ServerName, Proxy, ServerPort, ClientPort, ThriftSvc, Mode) ->
  GenServOpts = [],
  InitArgs = {Proxy, ServerPort, ClientPort, ThriftSvc, Mode},
  gen_server:start_link({local, ServerName}, ?MODULE, InitArgs, 
    GenServOpts).

stop(ServerName) ->
    gen_server:call(ServerName, stop).

%% handle_function returns ThriftResponse.
handle_function(ServerName, Fun, Args) ->
  gen_server:call(ServerName, {handle_function, Fun, Args}).

%% handle_function_cast returns ThriftResponse. It uses ox_thrift_conn:cast 
%% instead of ox_thrift_conn:call.
handle_function_cast(ServerName, Fun, Args) ->
  gen_server:call(ServerName, {handle_function_cast, Fun, Args}).

set_adtype(ServerName, NewAdType) ->
  gen_server:call(ServerName, {set_adtype, NewAdType}).

get_adtype(ServerName) ->
  gen_server:call(ServerName, get_adtype).

%% Making request using ox_thrift_conn:call from recordings; return the 
%% Thrift response.
make_request_call(ServerName, AdType, Fun) ->
  gen_server:call(ServerName, {make_request_call, AdType, Fun}).

make_request_cast(ServerName, AdType, Fun) ->
  gen_server:call(ServerName, {make_request_cast, AdType, Fun}).

%%====================================================================
%% Utility API
%%====================================================================

%% The DictN-th element of tuple Arg is the context dictionary. Args 
%% Need to contain KeysToRemove (list) or it will lager a warning.
%% The returned TrimmedArgs will have KeysToRemove erased.
trim_args(Args, DictN, KeysToRemove)
    when is_tuple(Args) and is_integer(DictN) and is_list(KeysToRemove) ->
  % Get context dictionary from tuple
  Dict = element(DictN, Args),

  % Trim dictionary
  TrimmedDict = trim_args_helper(Dict, KeysToRemove), 

  % Put the trimmed dictionary back to 
  setelement(DictN, Args, TrimmedDict).


%% trim_args_helper is used instead of trim_args when Args is not in 
%% standard format (e.g. proxy_mops_ssrtb)
trim_args_helper(Dict, KeysToRemove) 
    when is_list(KeysToRemove) ->

  % Erase the item from dictionary D if the key K present; give warning 
  % if it's not.
  CheckErase = 
    fun(K, D) ->
      case dict:is_key(K, D) of
        true ->
          dict:erase(K, D);
        _ ->
          lager:warning("Key ~p does not exist.", [K]),
          D
      end
    end,

  lists:foldl(CheckErase, Dict, KeysToRemove).

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
init({Proxy, ServerPort, ClientPort, ThriftSvc, Mode}) ->
  
  process_flag(trap_exit, true),

  % 4 servers are to be created with monitors.
  ProxyClientName   =  list_to_atom(atom_to_list(Proxy) ++ "_client"),
  LogServerName  =  list_to_atom(atom_to_list(Proxy) ++ "_log"),

  State = #state{proxy_name         = Proxy,
                 proxy_client_name  = ProxyClientName,
                 thrift_svc         = ThriftSvc,
                 server_port        = ServerPort,
                 client_port        = ClientPort,
                 log_server         = LogServerName,
                 mode               = Mode},

  start_proxy(State), % start_proxy does not modify State
  lager:debug("Proxy server ~p init/1 finished.", [Proxy]),

  {ok, State}.


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

%% Forward call only in proxy mode
handle_call({handle_function, Fun, Args}, _From, State = #state{mode = proxy}) ->
    {reply, forward_fun_call(Fun, Args, State, _LogResult = true), State};

handle_call({handle_function_cast, Fun, Args}, _From, 
      State = #state{mode = proxy}) ->
    {reply, forward_fun_cast(Fun, Args, State, _LogResult = true), State};

%% Replay Thrift response in replay_reply mode
handle_call({handle_function, Fun, Args}, _From, 
      State = #state{mode = replay_reply}) ->
    {reply, replay_response(Fun, Args, State), State};

handle_call({handle_function_cast, Fun, Args}, _From, 
      State = #state{mode = replay_reply}) ->
    {reply, replay_response(Fun, Args, State), State};

%% Replay Thrift request in replay_request
handle_call({make_request_call, AdType, Fun}, _From, 
      State = #state{mode = replay_request}) ->
    {reply, _ThriftResponse = replay_fun_call(AdType, Fun, State, call), State};
    
handle_call({make_request_cast, AdType, Fun}, _From, 
      State = #state{mode = replay_request}) ->
    {reply, _ThriftResponse = replay_fun_call(AdType, Fun, State, cast), State};

%% AdType operations
handle_call({set_adtype, NewAdType}, _From, State) ->
    {reply, ok, State#state{adtype=NewAdType}};

handle_call(get_adtype, _From, State=#state{proxy_name=Proxy, adtype=AdType}) ->
    Ret = string_format("~p -- adtype = ~p", [Proxy, AdType]),
    {reply, Ret, State};

%% Anything else...
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
%handle_cast(stop, State) ->
%  {stop, normal, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(normal, #state{proxy_name=ProxyName}) ->
    lager:info("Proxy server ~p is shutting down.", [ProxyName]),
    ok;
terminate(shutdown, State) ->
    terminate(normal, State).

%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%====================================================================
%% Internal Functions 
%%====================================================================

%%--------------------------------------------------------------------
%% Initialize proxy: 
%%    1. Establishing server-side / client-side connection.
%%    2. Create log server.
%%--------------------------------------------------------------------
start_proxy(State = #state{mode = Mode}) ->

  case Mode of
    proxy ->
      open_server_socket(State),
      connect_client(State);

    replay_reply ->
      open_server_socket(State);

    replay_request ->
      connect_client(State)
  end,

  % all 3 modes need log server (to write to or replay from)
  start_log_server(State),
  ok.


%%--------------------------------------------------------------------
%% Open the server-side Thrift socket
%%--------------------------------------------------------------------
open_server_socket(#state{proxy_name  = ProxyName,
                          thrift_svc  = ThriftSvc,
                          server_port = ServerPort
                         }) ->
  %% IsFramed should be true except for ssRtbService_thrift, which uses
  %% unbuffered transport frame.
  IsFramed = 
    case ThriftSvc of
      ssRtbService_thrift ->
        false;
      _Else ->
        true
    end,

  % Start the server
  {ok, ServerPid} = thrift_socket_server:start (
      [{port, ServerPort},
       %% {local, ProxyName} will be a registered process. Needs to be the
       %% same as handler
       {name, ProxyName}, 
       {service, ThriftSvc},
       {handler, ProxyName},  %% Handler:handle_function/2 will be called
       {framed, IsFramed},    %% buffered transport
       {socket_opts, [{recv_timeout, 60*60*1000}]}]),

  % This was a stupid idea...
  % The proxy is dependent on the Thrift server. thrift_socket_server:start
  % already restart itself when socket/acceptor dies, but this is just in 
  % case.
  link(ServerPid),
  lager:debug("Proxy ~p opened thrift socket on port ~p, " ++ 
              "Thrift service = ~p, handler = ~p, framed = ~p.",
              [ProxyName, ServerPort, ThriftSvc, ProxyName, IsFramed]).

%%--------------------------------------------------------------------
%% Open the client-side connection through ox_thrift_conn
%%--------------------------------------------------------------------
connect_client(#state{proxy_name           = ProxyName,
                      proxy_client_name    = ProxyClientName,
                      thrift_svc           = ThriftSvc,
                      client_port          = ClientPort}) ->
  %% IsFramed should be true except for ssRtbService_thrift, which uses
  %% unbuffered transport frame.
  IsFramed = 
    case ThriftSvc of
      ssRtbService_thrift ->
        false;
      _Else ->
        true
    end,

  Args = [ localhost,
           ClientPort,
           ThriftSvc,
           [ {framed, IsFramed} ], % thrift options
           _ReconnMin = 250,                 % ReconnMin
           _ReconnMax = 60*1000,             % ReconnMax
           _MondemandProgId = ProxyName ], 

  %% Use gen_server_pool so we can name the gen_server ClientPoolId.
  %% (ox_thrift_conn doesn't allow user-defined server name.)

  PoolOpts = [ { max_pool_size, 1 },
               { idle_timeout, 60*60 },
               { max_queue, 100 },
               { prog_id, ProxyName },  % For mondemand
               { pool_id, ProxyClientName} ],

  %% Ignore returned Pid. Will use ClientPoolId to call the client
  %% gen_server(_pool).
  _ClientPid = gen_server_pool:start_link( { local, ProxyClientName }, 
      ox_thrift_conn, Args, [], PoolOpts ),

  lager:debug("ox_thrift_conn client opened at port ~p.", [ClientPort]).


%%--------------------------------------------------------------------
%% Start the log server, which records (write) in proxy mode, but replay
%% (read) in either replay mode.
%%--------------------------------------------------------------------
start_log_server(#state{proxy_name = P, log_server = L, mode = M}) ->

  case M of 
    proxy ->
      RecDir = thrift_proxy_app:get_env_var(log_dir),
      LogFile = filename:join(RecDir, atom_to_list(P) ++ ".log"),
      erlterm2file:start_link(L, LogFile);
      
    _ReplayMode ->
      RecDir =thrift_proxy_app:get_env_var(rec_log_dir),
      LogFile = filename:join(RecDir, atom_to_list(P) ++ ".log"),
      ts_static_data:start_link(L, LogFile)
  end,
  ok.


%%--------------------------------------------------------------------
%% Replay the recorded call.
%%--------------------------------------------------------------------
replay_response(Fun, Args,
                #state{proxy_name         = ProxyName,
                       log_server         = LogServer}) ->

  lager:info("Proxy ~p replaying Thrift response to fun ~p", 
             [ProxyName, Fun]),
  % Trim away timestamp, trax.id, etc
  TrimmedArgs = ProxyName:trim_args(Fun, Args),
  ThriftResponse = 
    ts_static_data:lookup(LogServer, fun_args, Fun, TrimmedArgs),

  case ThriftResponse of
    {error, _Reason} ->
        lager:warning("No matching key in ets. Key(fun_args) = ~p", 
          [#fun_args{fct=Fun, trimmed_args=TrimmedArgs}]);
    Else ->
        Else
  end.



%%--------------------------------------------------------------------
%% Forward the call and record the Thrift request and response.
%%--------------------------------------------------------------------
forward_fun_call(Fun, Args,
                #state{proxy_name         = ProxyName,
                       proxy_client_name  = ProxyClientName,
                       log_server         = LogServer,
                       adtype             = AdType
                },
                LogResult) when is_boolean(LogResult) ->
  lager:info("~p making call: Fun = ~p, adtype = ~p.", 
             [ProxyName, Fun, AdType]),
  
  % gateway has the longest timeout (currently 5 seconds).
  {ok, Timeout} = oxcon:get_conf(ox_http_gateway, thrift_timeout),

  ThriftResponse =
    case catch ox_thrift_conn:call (ProxyClientName, Fun,
      tuple_to_list(Args), Timeout) of
      {ok, T} -> 
        lager:debug("~p: Thrift call finish normally.", [ProxyName]),
        {reply, T};
      {_Error, {timeout, _CallStack}} ->
        lager:error("~p: Thrift call finish WITH ERROR: timeout.", [ProxyName]),
        {error, timeout};
      {'EXIT', {{case_clause, {error,closed}},_}} ->
        lager:error("~p: Thrift call finish WITH ERROR: closed.", [ProxyName]),
        {error, closed};
      Error ->
        lager:error("~p: Thrift call finish WITH ERROR: ~p.", [ProxyName, Error]),
        {error, Error}
    end,
  
  % Log the result when ThriftResponse is good and LogResult is true.
  case {ThriftResponse, LogResult} of
    {{reply, _}, true} ->
      record_results(LogServer, ProxyName, Fun, Args, AdType, 
        ThriftResponse);
    _Else ->
      ok
  end,

  ThriftResponse.


%%--------------------------------------------------------------------
%% Forward the cast call and record the Thrift request and response.
%% review: This function is similar to forward_fun_call, but just different
%%         enough that it's cleaner to be a separate function. Oh well....
%%--------------------------------------------------------------------
forward_fun_cast(Fun, Args,
                #state{proxy_name         = ProxyName,
                       proxy_client_name  = ProxyClientName,
                       log_server         = LogServer,
                       adtype             = AdType
                }, 
                LogResult) when is_boolean(LogResult) ->
  ThriftResponse =
    case catch ox_thrift_conn:cast (ProxyClientName, Fun,
      tuple_to_list(Args)) of
      ok ->
        lager:debug("~p: Thrift cast finish normally.", [ProxyName]),
        ok;
      Error ->
        lager:debug("~p: Thrift cast finish with error: ~p.", [ProxyName,
          Error]),
        Error
    end,

  % Log the result when ThriftResponse is good and LogResult is true.
  case {ThriftResponse, LogResult} of
    {ok, true} ->
      record_results(LogServer, ProxyName, Fun, Args, AdType, 
        ThriftResponse);
    _Else ->
      ok
  end,
  
  ThriftResponse.


%%--------------------------------------------------------------------
%% Record the results.
%%--------------------------------------------------------------------
record_results(LogServer, ProxyName, Fun, Args, AdType, ThriftResponse) ->
  % log the results
  lager:debug("~p: Log request and response.", [ProxyName]),
  TrimmedArgs = ProxyName:trim_args(Fun, Args),
  erlterm2file:log(LogServer, 
    #fun_call{adtype=AdType, 
      fa=#fun_args{fct=Fun, trimmed_args=TrimmedArgs}, 
      full_args=Args, resp=ThriftResponse}).



%%--------------------------------------------------------------------
%% Replay the Thrift request.
%%--------------------------------------------------------------------
replay_fun_call(AdType, Fun,
                State = #state{log_server = LogServer},
                CallType) ->

  %% Get the Args
  Args = ts_static_data:lookup(LogServer, adtype_fct, AdType, Fun),
  %lager:debug("replaying function = ~p with args = ~p.", [Fun, Args]),
  case CallType of 
    call ->
      forward_fun_call(Fun, Args, State#state{adtype=AdType}, 
                       _LogResult = false);
    cast ->
      forward_fun_cast(Fun, Args, State#state{adtype=AdType}, 
                       _LogResult = false)
  end.
  
  

%%--------------------------------------------------------------------
%% string_format/2
%% Like io:format except it returns the evaluated string rather than write
%% it to standard output.
%% Parameters:
%%   1. format string similar to that used by io:format.
%%   2. list of values to supply to format string.
%% Returns:
%%   Formatted string.
%%--------------------------------------------------------------------
string_format(Pattern, Values) ->
     lists:flatten(io_lib:format(Pattern, Values)).
