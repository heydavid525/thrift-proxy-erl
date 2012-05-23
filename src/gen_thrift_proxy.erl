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
-export([start_link/5, 
         stop/1,
         handle_function/3,
         set_adtype/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
        terminate/2, code_change/3]).

-record(state, {proxy_name,
                proxy_client_name,
                thrift_svc,
                server_port,
                client_port,
                req_log_server,
                resp_log_server,
                adtype = afr_flash  % default ad type.
        }).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_link() -> {ok,Pid} | ignore | {error,Error}
%% Description: Starts the server
%%--------------------------------------------------------------------
% Proxy needs to be the module name.
start_link(ServerName, Proxy, ServerPort, ClientPort, ThriftSvc) ->
  GenServOpts = [],
  InitArgs = {Proxy, ServerPort, ClientPort, ThriftSvc},
  gen_server:start_link({local, ServerName}, ?MODULE, InitArgs, 
    GenServOpts).

stop(ServerName) ->
    gen_server:call(ServerName, stop).

% handle_function returns ThriftResponse.
handle_function(ServerName, Fun, Args) ->
  gen_server:call(ServerName, {handle_function, Fun, Args}).

set_adtype(ServerName, NewAdType) ->
  gen_server:call(ServerName, {set_adtype, NewAdType}).


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
init({Proxy, ServerPort, ClientPort, ThriftSvc}) ->
  
  process_flag(trap_exit, true),

  % 4 servers are to be created with monitors.
  %ProxyServerName   =  list_to_atom(atom_to_list(Proxy) ++ "_server"),
  ProxyClientName   =  list_to_atom(atom_to_list(Proxy) ++ "_client"),
  ReqLogServerName  =  list_to_atom(atom_to_list(Proxy) ++ "_req"),
  RespLogServerName =  list_to_atom(atom_to_list(Proxy) ++ "_resp"),

  State = #state{proxy_name         = Proxy,
                 proxy_client_name = ProxyClientName,
                 thrift_svc         = ThriftSvc,
                 server_port        = ServerPort,
                 client_port        = ClientPort,
                 req_log_server     = ReqLogServerName,
                 resp_log_server    = RespLogServerName},

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
handle_call({handle_function, Fun, Args}, _From, State) ->
    {reply, forward_fun_call(Fun, Args, State), State};
handle_call({set_adtype, NewAdType}, _From, State) ->
    {reply, ok, State#state{adtype=NewAdType}};
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

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
start_proxy(#state{proxy_name=ProxyName,
                   proxy_client_name=ProxyClientName,
                   thrift_svc=ThriftSvc,
                   server_port=ServerPort,
                   client_port=ClientPort,
                   req_log_server=LogServerReq,
                   resp_log_server=LogServerResp
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

  lager:debug("~p:start_proxy -- opening thrift socket on port ~p, Thrift
service=~p, handler=~p, framed=~p.",
    [?MODULE, ServerPort, ThriftSvc, ProxyName, IsFramed]),

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
  lager:debug("Thrift socket opened."),
  
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

  lager:debug("ox_thrift_conn server opened."),

  %% Start the logging facilities
  LogDir = get_env_var(log_dir),
  LogFileReq = 
    filename:join(LogDir, atom_to_list(ProxyName) ++ "_req.log"),
  LogFileResp = 
    filename:join(LogDir, atom_to_list(ProxyName) ++ "_resp.log"),
  erlterm2file:start_link(LogServerReq, LogFileReq),
  erlterm2file:start_link(LogServerResp, LogFileResp),

  ok.


%% Forward the call and record the Thrift request and response.
forward_fun_call(Function, Args,
                #state{proxy_name=ProxyName,
                       proxy_client_name=ProxyClientName,
                       req_log_server=LogServerReq,
                       resp_log_server=LogServerResp,
                       adtype=AdType
                }) ->
  % display adtype only for one proxy.
  if ProxyName =:= proxy_gw_ads -> lager:info("adtype = ~p.", [AdType]);
     true -> ok
  end,
  
  % templated after make_call in ox_broker_client
  {ok, Timeout} = oxcon:get_conf(ox_http_gateway, thrift_timeout),
  %lager:debug("gateway timeout = ~p ms.", [Timeout]),
  ThriftResponse =
    case catch ox_thrift_conn:call (ProxyClientName, Function,
      tuple_to_list(Args), Timeout) of
      {ok, T} -> 
        lager:debug("~p: Thrift call finish normally.", [ProxyName]),
        T;
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
  
  %% Log only if the call finishes successfully.
  case ThriftResponse of
    {error, _} ->
      ok;
    _Else ->
      lager:debug("~p: Log request and response.", [ProxyName]),
      erlterm2file:log(LogServerReq, 
        #fun_call{adtype=AdType, fct=Function, args=Args}),
      erlterm2file:log(LogServerResp, ThriftResponse)
  end,

  ThriftResponse.

%%====================================================================
%% Private Functions 
%%====================================================================
%% Get application environment variables.
get_env_var(Var) ->
  {ok, Val} = application:get_env(Var),
  Val.
