RANDOMSRC = ../../Godeps/_workspace/src/github.com/jbenet/go-random
IMAGE_NAME = ipfs-test-latest
IPFS_ROOT = ../..

test: clean setup
	./run-test-on-img.sh $(IMAGE_NAME)

setup: docker_ipfs_image data/filetiny data/filerand

save_logs:
	sh bin/save_logs.sh

save_profiling_data:
	sh bin/save_profiling_data.sh

data/filetiny: Makefile
	cp Makefile ./data/filetiny # simple

data/filerand: bin/random
	./bin/random 50000000 > ./data/filerand

bin/random: $(RANDOMSRC)/**/*.go
	go build -o ./bin/random $(RANDOMSRC)/random

# just build it every time... this part isn't
# even the lengthy part, and it decreases pain.
docker_ipfs_image:
	docker build -t $(IMAGE_NAME) -f Dockerfile.fast .
	docker images | grep $(IMAGE_NAME)

clean:
	sh bin/clean.sh
	fig stop
	fig rm -v --force
	rm -f bin/random
	rm -f data/filetiny
	rm -f data/filerand
	rm -rf build/*
	docker rmi $(docker images | grep "^<none>" | awk "{print \$3}") -f || true
