package natpmp

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

type callRecord struct {
	// The expected msg argument to call.
	msg    []byte
	result []byte
	err    error
}

type mockNetwork struct {
	// test object, used to report errors.
	t  *testing.T
	cr callRecord
}

func (n *mockNetwork) call(msg []byte, timeout time.Duration) (result []byte, err error) {
	if bytes.Compare(msg, n.cr.msg) != 0 {
		n.t.Errorf("msg=%v, expected %v", msg, n.cr.msg)
	}
	return n.cr.result, n.cr.err
}

type getExternalAddressRecord struct {
	result *GetExternalAddressResult
	err    error
	cr     callRecord
}

/*

Poor-man's generater code

import "net"

type callRecorder struct {
    callRecord callRecord
}

func (cr *callRecorder) observeCall(msg []byte, result []byte, err error) {
    cr.callRecord = callRecord{msg, result, err}
}

func TestRecordGetExternalAddress(t *testing.T) {
    cr := &callRecorder{}
    c := Client{&recorder{&network{net.IPv4(192,168,1,1)},cr}}
    result, err := c.GetExternalAddress()
    t.Logf("%#v, %#v, %#v", result, err, cr.callRecord)
}

func TestRecordAddPortMapping(t *testing.T) {
    cr := &callRecorder{}
    c := Client{&recorder{&network{net.IPv4(192,168,1,1)},cr}}
    result, err := c.AddPortMapping("tcp", 123, 0, 0)
    t.Logf("%#v, %#v, %#v", result, err, cr.callRecord)
}

*/

func TestGetExternalAddress(t *testing.T) {
	dummyError := fmt.Errorf("dummy error")
	testCases := []getExternalAddressRecord{
		{
			nil,
			dummyError,
			callRecord{[]uint8{0x0, 0x0}, nil, dummyError},
		},
		{
			&GetExternalAddressResult{0x13f24f, [4]uint8{0x49, 0x8c, 0x36, 0x9a}},
			nil,
			callRecord{[]uint8{0x0, 0x0}, []uint8{0x0, 0x80, 0x0, 0x0, 0x0, 0x13, 0xf2, 0x4f, 0x49, 0x8c, 0x36, 0x9a}, nil},
		},
	}
	for i, testCase := range testCases {
		t.Logf("case %d", i)
		c := Client{&mockNetwork{t, testCase.cr}, 0}
		result, err := c.GetExternalAddress()
		if err != nil {
			if err != testCase.err {
				t.Error(err)
			}
		} else {
			if result.SecondsSinceStartOfEpoc != testCase.result.SecondsSinceStartOfEpoc {
				t.Errorf("result.SecondsSinceStartOfEpoc=%d != %d", result.SecondsSinceStartOfEpoc, testCase.result.SecondsSinceStartOfEpoc)
			}
			if bytes.Compare(result.ExternalIPAddress[:], testCase.result.ExternalIPAddress[:]) != 0 {
				t.Errorf("result.ExternalIPAddress=%v != %v", result.ExternalIPAddress, testCase.result.ExternalIPAddress)
			}
		}
	}
}

type addPortMappingRecord struct {
	protocol              string
	internalPort          int
	requestedExternalPort int
	lifetime              int
	result                *AddPortMappingResult
	err                   error
	cr                    callRecord
}

func TestAddPortMapping(t *testing.T) {
	dummyError := fmt.Errorf("dummy error")
	testCases := []addPortMappingRecord{
		// Propagate error
		{
			"udp", 123, 456, 1200,
			nil,
			dummyError,
			callRecord{[]uint8{0x0, 0x1, 0x0, 0x0, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0}, nil, dummyError},
		},
		// Add UDP
		{
			"udp", 123, 456, 1200,
			&AddPortMappingResult{0x13feff, 0x7b, 0x1c8, 0x4b0},
			nil,
			callRecord{
				[]uint8{0x0, 0x1, 0x0, 0x0, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				[]uint8{0x0, 0x81, 0x0, 0x0, 0x0, 0x13, 0xfe, 0xff, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				nil,
			},
		},
		// Add TCP
		{
			"tcp", 123, 456, 1200,
			&AddPortMappingResult{0x140321, 0x7b, 0x1c8, 0x4b0},
			nil,
			callRecord{
				[]uint8{0x0, 0x2, 0x0, 0x0, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				[]uint8{0x0, 0x82, 0x0, 0x0, 0x0, 0x14, 0x3, 0x21, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				nil,
			},
		},
		// Remove UDP
		{
			"udp", 123, 0, 0,
			&AddPortMappingResult{0x1403d5, 0x7b, 0x0, 0x0},
			nil,
			callRecord{
				[]uint8{0x0, 0x1, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				[]uint8{0x0, 0x81, 0x0, 0x0, 0x0, 0x14, 0x3, 0xd5, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				nil,
			},
		},
		// Remove TCP
		{
			"tcp", 123, 0, 0,
			&AddPortMappingResult{0x140496, 0x7b, 0x0, 0x0},
			nil,
			callRecord{
				[]uint8{0x0, 0x2, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				[]uint8{0x0, 0x82, 0x0, 0x0, 0x0, 0x14, 0x4, 0x96, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				nil,
			},
		},
	}

	for i, testCase := range testCases {
		t.Logf("case %d", i)
		c := Client{&mockNetwork{t, testCase.cr}, 0}
		result, err := c.AddPortMapping(testCase.protocol, testCase.internalPort, testCase.requestedExternalPort, testCase.lifetime)
		if err != nil || testCase.err != nil {
			if err != testCase.err && fmt.Sprintf("%v", err) != fmt.Sprintf("%v", testCase.err) {
				t.Errorf("err=%v != %v", err, testCase.err)
			}
		} else {
			if result.SecondsSinceStartOfEpoc != testCase.result.SecondsSinceStartOfEpoc {
				t.Errorf("result.SecondsSinceStartOfEpoc=%d != %d", result.SecondsSinceStartOfEpoc, testCase.result.SecondsSinceStartOfEpoc)
			}
			if result.InternalPort != testCase.result.InternalPort {
				t.Errorf("result.InternalPort=%d != %d", result.InternalPort, testCase.result.InternalPort)
			}
			if result.MappedExternalPort != testCase.result.MappedExternalPort {
				t.Errorf("result.InternalPort=%d != %d", result.MappedExternalPort, testCase.result.MappedExternalPort)
			}
			if result.PortMappingLifetimeInSeconds != testCase.result.PortMappingLifetimeInSeconds {
				t.Errorf("result.InternalPort=%d != %d", result.PortMappingLifetimeInSeconds, testCase.result.PortMappingLifetimeInSeconds)
			}
		}
	}
}

func TestProtocolChecks(t *testing.T) {
	testCases := []addPortMappingRecord{
		// Unexpected result size.
		{
			"tcp", 123, 456, 1200,
			nil,
			fmt.Errorf("unexpected result size %d, expected %d", 1, 16),
			callRecord{
				[]uint8{0x0, 0x2, 0x0, 0x0, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				[]uint8{0x0},
				nil,
			},
		},
		//  Unknown protocol version.
		{
			"tcp", 123, 456, 1200,
			nil,
			fmt.Errorf("unknown protocol version %d", 1),
			callRecord{
				[]uint8{0x0, 0x2, 0x0, 0x0, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				[]uint8{0x1, 0x82, 0x0, 0x0, 0x0, 0x14, 0x4, 0x96, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				nil,
			},
		},
		// Unexpected opcode.
		{
			"tcp", 123, 456, 1200,
			nil,
			fmt.Errorf("Unexpected opcode %d. Expected %d", 0x88, 0x82),
			callRecord{
				[]uint8{0x0, 0x2, 0x0, 0x0, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				[]uint8{0x0, 0x88, 0x0, 0x0, 0x0, 0x14, 0x4, 0x96, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				nil,
			},
		},
		// Non-success result code.
		{
			"tcp", 123, 456, 1200,
			nil,
			fmt.Errorf("Non-zero result code %d", 17),
			callRecord{
				[]uint8{0x0, 0x2, 0x0, 0x0, 0x0, 0x7b, 0x1, 0xc8, 0x0, 0x0, 0x4, 0xb0},
				[]uint8{0x0, 0x82, 0x0, 0x11, 0x0, 0x14, 0x4, 0x96, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
				nil,
			},
		},
	}
	for i, testCase := range testCases {
		t.Logf("case %d", i)
		c := Client{&mockNetwork{t, testCase.cr}, 0}
		result, err := c.AddPortMapping(testCase.protocol, testCase.internalPort, testCase.requestedExternalPort, testCase.lifetime)
		if err != testCase.err && fmt.Sprintf("%v", err) != fmt.Sprintf("%v", testCase.err) {
			t.Errorf("err=%v != %v", err, testCase.err)
		}
		if result != nil {
			t.Errorf("result=%v != nil", result)
		}
	}
}
