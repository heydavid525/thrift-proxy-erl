-ifndef(_proxy_types_included).
-define(_proxy_types_included, yeah).

-record(fun_call, 
        {adtype :: atom(),
         fct    :: atom(),
         args,
         resp}).

-endif.
