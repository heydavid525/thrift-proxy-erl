%%%-------------------------------------------------------------------
%%% File    : thrift_proxy_sup.erl
%%% Author  : Dai Wei <dai.wei@openx.com>
%%% Description :
%%%   Root supervisor responsible for starting the modules in the app.
%%%
%%% Created : 15 May 2012 by Dai Wei <dai.wei@openx.com>
%%%-------------------------------------------------------------------

-module(thrift_proxy_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
start_link() ->
  supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------
init([]) ->
  Children = [ generate_child_spec(Proxy) || 
      Proxy <- thrift_proxy_app:get_env_var(proxy_list) ],

  %% Disallow automatic restart
  RestartStrategy = {one_for_one, 0, 1},
  {ok, {RestartStrategy, Children}}.

%%--------------------------------------------------------------------
%% Internal Functions 
%%--------------------------------------------------------------------

%% Since all 4 proxies are similar, DRY out the code.
generate_child_spec(Module) ->
  ReplayVar = list_to_atom(atom_to_list(Module) ++ "_replay"),
  Replay = thrift_proxy_app:get_env_var(ReplayVar),
  lager:info("proxy ~p has replay mode = ~p", [Module, Replay]),
  StartLinkArgs = [Replay],
  {Module, {Module, start_link, StartLinkArgs},
   permanent, 20000, worker, [Module]}.
