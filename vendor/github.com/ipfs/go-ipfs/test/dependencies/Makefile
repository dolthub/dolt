
all: restore

restore:
	@echo "*** $@ ***"
	which godep
	mkdir -p tmp_gopath
	OLD_GOPATH="$$GOPATH"
	export GOPATH=$$(pwd)/tmp_gopath
	cd ../..
	godep restore
	cd -
	rm -rf tmp_gopath
	export GOPATH="$$OLD_GOPATH"

.PHONY: all restore
