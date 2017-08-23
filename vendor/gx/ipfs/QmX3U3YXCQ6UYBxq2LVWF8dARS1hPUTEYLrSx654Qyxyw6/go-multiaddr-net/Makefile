export PATH := bin:$(PATH)

deps: gxbins
	bin/gx install --global
	bin/gx-go rewrite

publish:
	bin/gx-go rewrite --undo
	bin/gx publish

test: deps
	go test -race -cpu=5 -v ./...

gxbins: bin/gx-v0.6.0 bin/gx-go-v1.2.0

bin/gx-v0.6.0:
	mkdir -p bin
	./bin/dist_get gx bin/gx-v0.6.0 v0.6.0
	ln -s gx-v0.6.0 bin/gx

bin/gx-go-v1.2.0:
	mkdir -p bin
	./bin/dist_get gx-go bin/gx-go-v1.2.0 v1.2.0
	ln -s gx-go-v1.2.0 bin/gx-go

clean:
	rm -f bin/gx*
	rm -rf bin/tmp
