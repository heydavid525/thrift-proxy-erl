%%%-------------------------------------------------------------------
%%% File    : proxy_ads_mds.erl
%%% Author  : Dai Wei <dai.wei@openx.com>
%%% Description :
%%%   Thrift callback uses Handler:handle_function. This module 
%%%   mainly provides that plus proxy-specific info on top of 
%%%   proxy_base.erl.
%%%
%%% Created : 15 May 2012 by Dai Wei <dai.wei@openx.com>
%%%-------------------------------------------------------------------

-module(proxy_ads_mds).

%% User defined macros:
-define(THRIFT_SVC, deliveryService_thrift).

%% API
-export([start_link/0,
         start_link/1,
         get_adtype/0,
         set_adtype/1]).

%% gen_thrift_proxy callbacks
-export([trim_args/2]).

%% Thrift callbacks
-export([stop/1,
         handle_function/2]).

%% autogenerated macros:
-define(SERVER_NAME, list_to_atom(atom_to_list(?MODULE) ++ "_server")).

%%====================================================================
%% API
%%====================================================================
% Default replay to false (a normal proxy)
start_link() ->
  start_link(false).

start_link(Replay) when is_boolean(Replay) ->
  ServerPortEnv = list_to_atom(atom_to_list(?MODULE) ++ "_server_port"),
  ServerPort = thrift_proxy_app:get_env_var(ServerPortEnv),
  ClientPortEnv = list_to_atom(atom_to_list(?MODULE) ++ "_client_port"),
  ClientPort = thrift_proxy_app:get_env_var(ClientPortEnv),
  gen_thrift_proxy:start_link(?SERVER_NAME, 
      ?MODULE, ServerPort, ClientPort, ?THRIFT_SVC, Replay).

set_adtype(NewAdType) ->
  gen_thrift_proxy:set_adtype(?SERVER_NAME, NewAdType).

get_adtype() ->
  gen_thrift_proxy:get_adtype(?SERVER_NAME).

%%====================================================================
%% gen_thrift_proxy callback functions
%%====================================================================
% Trim away timestamp, trax.id, etc. Hack hack hack!
trim_args(Fun, Args) ->
  if 
    Fun =:= requestAds ->
      KeysToRemove = [<<"ox.internal.timestamp">>, 
                      <<"ox.internal.trax_id">>],
      gen_thrift_proxy:trim_args(Args, 5, KeysToRemove);
    Fun =:= recordEvent -> 
      KeysToRemove = [],
      gen_thrift_proxy:trim_args(Args, 6, KeysToRemove)
  end.
  

%%====================================================================
%% Thrift callback functions
%%====================================================================
handle_function (Function, Args) when is_atom(Function), is_tuple(Args) ->
  lager:info("~p:handle_function -- Function = ~p.", [?MODULE, Function]),
  gen_thrift_proxy:handle_function(?SERVER_NAME, Function, Args).

stop(Server) ->
  thrift_socket_server:stop(Server),
  ok.
