.PHONY: rel deps test

# fetches dependencies and compiles
all: deps compile

# compiles the codebase
compile: deps
	@./rebar3 compile

# tests the codebase
test:
	@./rebar3 eunit

# compiles the codebase but skips dependencies
app:
	@./rebar3 compile skip_deps=true

# fetches dependencies
deps:
	@./rebar3 get-deps

# cleans the rebar3 artifact folders
clean:
	@./rebar3 clean

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler mnesia public_key snmp

# runs dialyzer
dialyzer:
	./rebar3 dialyzer
