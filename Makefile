.PHONY: rel deps test

# fetches dependencies and compiles
all: deps compile

# compiles the codebase
compile: deps
	@escript rebar3 compile

# tests the codebase
test:
	@escript rebar3 eunit

# performs test coverage analysis
cover:
	@escript rebar3 cover

# compiles the codebase but skips dependencies
app:
	@escript rebar3 compile skip_deps=true

# fetches dependencies
deps:
	@escript rebar3 get-deps

# cleans the rebar3 artifact folders
clean:
	@escript rebar3 clean

# runs dialyzer
dialyzer:
	@escript rebar3 dialyzer
