# General tools

SHELL=PATH='$(PATH)' /bin/sh

PROTOC = protoc --gogofaster_out=. --proto_path=.:$(GOPATH)/src:$(dir $@) $<

# enable second expansion
.SECONDEXPANSION:

include Rules.mk
