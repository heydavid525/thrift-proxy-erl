-ifndef(_proxy_types_included).
-define(_proxy_types_included, yeah).

% Store the trimmed args (excluding timestamp, trax.id, etc).
-record(fun_args,
        {fct :: atom(),
         trimmed_args}).

% #fun_call.fa will be the key for ets:lookup
% since it uniquely determines the function call.
-record(fun_call, 
        {adtype :: atom(),
         fa=#fun_args{}, 
         full_args,   % full args for making original request.
         resp}).


-endif.
