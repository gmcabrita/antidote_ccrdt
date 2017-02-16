.PHONY: rel deps test

all: deps compile

compile: deps
	@./rebar3 compile

test:
	@./rebar3 eunit

app:
	@./rebar3 compile skip_deps=true

deps:
	@./rebar3 get-deps

clean:
	@./rebar3 clean

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler mnesia public_key snmp

typer:
	typer --annotate -I ../ --plt $(PLT) -r src

dialyzer:
	./rebar3 dialyzer
