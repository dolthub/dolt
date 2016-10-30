all: cstatfs bulkstat.bin

cstatfs: statfs.cc
	g++ -O2 -Wall -std=c++0x $< `pkg-config fuse --cflags --libs` -o $@

bulkstat.bin: bulkstat/main.go
	(cd bulkstat && go build main.go)
	cp bulkstat/main $@
