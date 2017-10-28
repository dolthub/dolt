package descriptor_test

import (
	"fmt"
	"testing"

	"gx/ipfs/QmXSs8cccbT4zDR95c1iRpYKDqVMzqeF1J6iZcavgE6eNw/protobuf/descriptor"
	tpb "gx/ipfs/QmXSs8cccbT4zDR95c1iRpYKDqVMzqeF1J6iZcavgE6eNw/protobuf/proto/testdata"
	protobuf "gx/ipfs/QmXSs8cccbT4zDR95c1iRpYKDqVMzqeF1J6iZcavgE6eNw/protobuf/protoc-gen-go/descriptor"
)

func TestMessage(t *testing.T) {
	var msg *protobuf.DescriptorProto
	fd, md := descriptor.ForMessage(msg)
	if pkg, want := fd.GetPackage(), "google.protobuf"; pkg != want {
		t.Errorf("descriptor.ForMessage(%T).GetPackage() = %q; want %q", msg, pkg, want)
	}
	if name, want := md.GetName(), "DescriptorProto"; name != want {
		t.Fatalf("descriptor.ForMessage(%T).GetName() = %q; want %q", msg, name, want)
	}
}

func Example_Options() {
	var msg *tpb.MyMessageSet
	_, md := descriptor.ForMessage(msg)
	if md.GetOptions().GetMessageSetWireFormat() {
		fmt.Printf("%v uses option message_set_wire_format.\n", md.GetName())
	}

	// Output:
	// MyMessageSet uses option message_set_wire_format.
}
