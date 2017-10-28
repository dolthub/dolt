
build:
	go build

install: build
	go install

examples: build
	cat examples/one.json | ./multicodec recode /cbor >examples/one.cbor
	cat examples/many.json | ./multicodec recode /cbor >examples/many.cbor

	cat examples/one.json >examples/four.mixed
	cat examples/one.cbor >>examples/four.mixed
	cat examples/one.json >>examples/four.mixed
	cat examples/one.cbor >>examples/four.mixed

	cat examples/four.mixed | ./multicodec recode /json >examples/four.unmixed.json
	cat examples/four.mixed | ./multicodec recode /cbor >examples/four.unmixed.cbor
	cat examples/four.mixed | ./multicodec headers >examples/four.headers
	cat examples/four.mixed | ./multicodec paths >examples/four.paths
	cat examples/four.headers | ./multicodec h2p >examples/four.paths2
	cat examples/four.paths | ./multicodec p2h >examples/four.headers2

	./multicodec header /json >examples/json_header
	./multicodec header /protobuf/msgio >examples/pb_header
	./multicodec header /multicodec/cbor >examples/cbor_header
