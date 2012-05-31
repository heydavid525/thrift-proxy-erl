%%%-------------------------------------------------------------------
%%% File    : thrift_proxy_app.erl
%%% Author  : Dai Wei <dai.wei@openx.com>
%%% Description :
%%%   The entry point of active application thrift_proxy_erl.
%%%
%%% Created : 15 May 2012 by Dai Wei <dai.wei@openx.com>
%%%-------------------------------------------------------------------
-module(thrift_proxy_app).

-behaviour(application).

%% API
-export([start_all/0,
         set_adtype/1,
         get_adtype/0,
         stop/0,
         get_env_var/1]).

%% Application callbacks
-export([start/2, stop/1]).

%% Should be the same as in .app file.
-define(APP_NAME, thrift_proxy_erl).

%%====================================================================
%% API 
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start_all
%% 
%% Description: Starts apps this depends on, then starts this
%%--------------------------------------------------------------------
start_all() ->
  [ ensure_started(App)
    || App <- [ sasl, lager, oxcon, thrift, lwes,
                mondemand, ?APP_NAME]
  ].

% Change the ad type for all proxy gen_server.
set_adtype(NewAdType) ->
  [ Mod:set_adtype(NewAdType) 
    || Mod <- get_env_var(proxy_list)
  ].

get_adtype() ->
  Result = [ Mod:get_adtype() 
    || Mod <- get_env_var(proxy_list)
  ],
  print_list_format(Result).

%get_proxy_list() ->
%  {ok, ProxyList} = application:get_env(?APP_NAME, proxy_list),
%  lager:debug("ProxyList = ~p", [ProxyList]),
%  ProxyList.

get_env_var(Var) when is_atom(Var) ->
  {ok, Val} = application:get_env(?APP_NAME, Var),
  Val.
  

stop() ->
  application:stop(?APP_NAME).

%%====================================================================
%% Application callbacks
%%====================================================================
%%--------------------------------------------------------------------
%% Function: start(Type, StartArgs) -> {ok, Pid} |
%%                                     {ok, Pid, State} |
%%                                     {error, Reason}
%% Description: This function is called whenever an application 
%% is started using application:start/1,2, and should start the processes
%% of the application. If the application is structured according to the
%% OTP design principles as a supervision tree, this means starting the
%% top supervisor of the tree.
%%--------------------------------------------------------------------
start(_Type, _StartArgs) ->
  {ok, LogLevel} = application:get_env(log_level),
  lager:notice("log_level = ~p.~n", [LogLevel]),
  lager:set_loglevel(lager_console_backend, LogLevel),
  case thrift_proxy_sup:start_link() of
    {ok, Pid} ->
      %% Set log level from app definition.
      {ok, Pid};
    Other ->
      {error, Other}
  end.

%%--------------------------------------------------------------------
%% Function: stop(State) -> void()
%% Description: This function is called whenever an application
%% has stopped. It is intended to be the opposite of Module:start/2 and
%% should do any necessary cleaning up. The return value is ignored. 
%%--------------------------------------------------------------------
stop(_State) ->
  ok.


%%====================================================================
%% Private Functions 
%%====================================================================
%% accept already_started error as ok.
ensure_started(App) ->
   case application:start(App) of
     ok ->
       ok;
     {error, {already_started, App}} ->
       ok
   end.

print_list_format([]) ->
  ok;
print_list_format([H | T]) ->
  io:format("~p~n", [H]),
  print_list_format(T).
