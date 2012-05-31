

compile:
	erlc -I include -I /usr/lib64/erlang/lib/ \
		-o ebin +'{parse_transform, lager_transform}' src/*.erl 

run:
	erl -pa ebin -s thrift_proxy_app start_all

clean:
	rm ebin/*.beam
