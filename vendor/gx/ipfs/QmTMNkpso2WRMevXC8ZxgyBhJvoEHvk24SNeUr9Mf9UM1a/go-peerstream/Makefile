test: gx-bins
	cd muxtest && gx install --global && gx-go rewrite && go test -v

test_race: deps
	cd muxtest && gx install --global && gx-go rewrite && go test -race -v

gx-bins:
	go get github.com/whyrusleeping/gx
	go get github.com/whyrusleeping/gx-go

deps: gx-bins
	gx --verbose install --global
	gx-go rewrite

clean: gx-bins
	gx-go rewrite --undo
	cd muxtest && gx-go rewrite --undo
