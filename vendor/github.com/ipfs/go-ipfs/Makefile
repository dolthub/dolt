# General tools

SHELL=PATH='$(PATH)' /bin/sh

PROTOC = protoc --gogo_out=. --proto_path=.:/usr/local/opt/protobuf/include:$(dir $@) $<

# enable second expansion
.SECONDEXPANSION:

include Rules.mk
