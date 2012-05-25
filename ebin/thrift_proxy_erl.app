{application,thrift_proxy_erl,
    [{description,"A thrift proxy"},
    {vsn,"0.0.0"},
    {modules,[erlterm2file,
        proxy_gw_ads]},
    {registered,[]},
    {applications,[kernel, 
                   stdlib, 
                   mondemand, 
                   oxcon, 
                   lager]},
    {env,[{proxy_list, [proxy_gw_ads, 
                        proxy_ads_mds 
                        %proxy_mds_mops, 
                        %proxy_mops_ssrtb
                       ]},
          {log_level, debug},
          {log_dir, "/home/produser/.thrift_proxy"}
          ]},
    {mod,{thrift_proxy_app,[]}}
    ]}.
