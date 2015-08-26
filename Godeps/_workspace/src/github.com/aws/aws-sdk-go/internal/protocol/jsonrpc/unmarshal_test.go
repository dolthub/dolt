package jsonrpc_test

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/request"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/service"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/aws/service/serviceinfo"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/internal/protocol/jsonrpc"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/internal/protocol/xml/xmlutil"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/aws/aws-sdk-go/internal/signer/v4"
	"github.com/attic-labs/noms/Godeps/_workspace/src/github.com/stretchr/testify/assert"
	"github.com/aws/aws-sdk-go/internal/util"
)

var _ bytes.Buffer // always import bytes
var _ http.Request
var _ json.Marshaler
var _ time.Time
var _ xmlutil.XMLNode
var _ xml.Attr
var _ = ioutil.Discard
var _ = util.Trim("")
var _ = url.Values{}
var _ = io.EOF

type OutputService1ProtocolTest struct {
	*service.Service
}

// New returns a new OutputService1ProtocolTest client.
func NewOutputService1ProtocolTest(config *aws.Config) *OutputService1ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "outputservice1protocoltest",
			APIVersion:   "",
			JSONVersion:  "",
			TargetPrefix: "",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &OutputService1ProtocolTest{service}
}

// newRequest creates a new request for a OutputService1ProtocolTest operation and runs any
// custom request initialization.
func (c *OutputService1ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opOutputService1TestCaseOperation1 = "OperationName"

// OutputService1TestCaseOperation1Request generates a request for the OutputService1TestCaseOperation1 operation.
func (c *OutputService1ProtocolTest) OutputService1TestCaseOperation1Request(input *OutputService1TestShapeOutputService1TestCaseOperation1Input) (req *request.Request, output *OutputService1TestShapeOutputService1TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opOutputService1TestCaseOperation1,
	}

	if input == nil {
		input = &OutputService1TestShapeOutputService1TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &OutputService1TestShapeOutputService1TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *OutputService1ProtocolTest) OutputService1TestCaseOperation1(input *OutputService1TestShapeOutputService1TestCaseOperation1Input) (*OutputService1TestShapeOutputService1TestCaseOperation1Output, error) {
	req, out := c.OutputService1TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type OutputService1TestShapeOutputService1TestCaseOperation1Input struct {
	metadataOutputService1TestShapeOutputService1TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataOutputService1TestShapeOutputService1TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService1TestShapeOutputService1TestCaseOperation1Output struct {
	metadataOutputService1TestShapeOutputService1TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataOutputService1TestShapeOutputService1TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService2ProtocolTest struct {
	*service.Service
}

// New returns a new OutputService2ProtocolTest client.
func NewOutputService2ProtocolTest(config *aws.Config) *OutputService2ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "outputservice2protocoltest",
			APIVersion:   "",
			JSONVersion:  "",
			TargetPrefix: "",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &OutputService2ProtocolTest{service}
}

// newRequest creates a new request for a OutputService2ProtocolTest operation and runs any
// custom request initialization.
func (c *OutputService2ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opOutputService2TestCaseOperation1 = "OperationName"

// OutputService2TestCaseOperation1Request generates a request for the OutputService2TestCaseOperation1 operation.
func (c *OutputService2ProtocolTest) OutputService2TestCaseOperation1Request(input *OutputService2TestShapeOutputService2TestCaseOperation1Input) (req *request.Request, output *OutputService2TestShapeOutputService2TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opOutputService2TestCaseOperation1,
	}

	if input == nil {
		input = &OutputService2TestShapeOutputService2TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &OutputService2TestShapeOutputService2TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *OutputService2ProtocolTest) OutputService2TestCaseOperation1(input *OutputService2TestShapeOutputService2TestCaseOperation1Input) (*OutputService2TestShapeOutputService2TestCaseOperation1Output, error) {
	req, out := c.OutputService2TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type OutputService2TestShapeOutputService2TestCaseOperation1Input struct {
	metadataOutputService2TestShapeOutputService2TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataOutputService2TestShapeOutputService2TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService2TestShapeOutputService2TestCaseOperation1Output struct {
	Char *string `type:"character"`

	Double *float64 `type:"double"`

	FalseBool *bool `type:"boolean"`

	Float *float64 `type:"float"`

	Long *int64 `type:"long"`

	Num *int64 `type:"integer"`

	Str *string `type:"string"`

	TrueBool *bool `type:"boolean"`

	metadataOutputService2TestShapeOutputService2TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataOutputService2TestShapeOutputService2TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService3ProtocolTest struct {
	*service.Service
}

// New returns a new OutputService3ProtocolTest client.
func NewOutputService3ProtocolTest(config *aws.Config) *OutputService3ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "outputservice3protocoltest",
			APIVersion:   "",
			JSONVersion:  "",
			TargetPrefix: "",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &OutputService3ProtocolTest{service}
}

// newRequest creates a new request for a OutputService3ProtocolTest operation and runs any
// custom request initialization.
func (c *OutputService3ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opOutputService3TestCaseOperation1 = "OperationName"

// OutputService3TestCaseOperation1Request generates a request for the OutputService3TestCaseOperation1 operation.
func (c *OutputService3ProtocolTest) OutputService3TestCaseOperation1Request(input *OutputService3TestShapeOutputService3TestCaseOperation1Input) (req *request.Request, output *OutputService3TestShapeOutputService3TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opOutputService3TestCaseOperation1,
	}

	if input == nil {
		input = &OutputService3TestShapeOutputService3TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &OutputService3TestShapeOutputService3TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *OutputService3ProtocolTest) OutputService3TestCaseOperation1(input *OutputService3TestShapeOutputService3TestCaseOperation1Input) (*OutputService3TestShapeOutputService3TestCaseOperation1Output, error) {
	req, out := c.OutputService3TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type OutputService3TestShapeBlobContainer struct {
	Foo []byte `locationName:"foo" type:"blob"`

	metadataOutputService3TestShapeBlobContainer `json:"-" xml:"-"`
}

type metadataOutputService3TestShapeBlobContainer struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService3TestShapeOutputService3TestCaseOperation1Input struct {
	metadataOutputService3TestShapeOutputService3TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataOutputService3TestShapeOutputService3TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService3TestShapeOutputService3TestCaseOperation1Output struct {
	BlobMember []byte `type:"blob"`

	StructMember *OutputService3TestShapeBlobContainer `type:"structure"`

	metadataOutputService3TestShapeOutputService3TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataOutputService3TestShapeOutputService3TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService4ProtocolTest struct {
	*service.Service
}

// New returns a new OutputService4ProtocolTest client.
func NewOutputService4ProtocolTest(config *aws.Config) *OutputService4ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "outputservice4protocoltest",
			APIVersion:   "",
			JSONVersion:  "",
			TargetPrefix: "",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &OutputService4ProtocolTest{service}
}

// newRequest creates a new request for a OutputService4ProtocolTest operation and runs any
// custom request initialization.
func (c *OutputService4ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opOutputService4TestCaseOperation1 = "OperationName"

// OutputService4TestCaseOperation1Request generates a request for the OutputService4TestCaseOperation1 operation.
func (c *OutputService4ProtocolTest) OutputService4TestCaseOperation1Request(input *OutputService4TestShapeOutputService4TestCaseOperation1Input) (req *request.Request, output *OutputService4TestShapeOutputService4TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opOutputService4TestCaseOperation1,
	}

	if input == nil {
		input = &OutputService4TestShapeOutputService4TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &OutputService4TestShapeOutputService4TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *OutputService4ProtocolTest) OutputService4TestCaseOperation1(input *OutputService4TestShapeOutputService4TestCaseOperation1Input) (*OutputService4TestShapeOutputService4TestCaseOperation1Output, error) {
	req, out := c.OutputService4TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type OutputService4TestShapeOutputService4TestCaseOperation1Input struct {
	metadataOutputService4TestShapeOutputService4TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataOutputService4TestShapeOutputService4TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService4TestShapeOutputService4TestCaseOperation1Output struct {
	StructMember *OutputService4TestShapeTimeContainer `type:"structure"`

	TimeMember *time.Time `type:"timestamp" timestampFormat:"unix"`

	metadataOutputService4TestShapeOutputService4TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataOutputService4TestShapeOutputService4TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService4TestShapeTimeContainer struct {
	Foo *time.Time `locationName:"foo" type:"timestamp" timestampFormat:"unix"`

	metadataOutputService4TestShapeTimeContainer `json:"-" xml:"-"`
}

type metadataOutputService4TestShapeTimeContainer struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService5ProtocolTest struct {
	*service.Service
}

// New returns a new OutputService5ProtocolTest client.
func NewOutputService5ProtocolTest(config *aws.Config) *OutputService5ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "outputservice5protocoltest",
			APIVersion:   "",
			JSONVersion:  "",
			TargetPrefix: "",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &OutputService5ProtocolTest{service}
}

// newRequest creates a new request for a OutputService5ProtocolTest operation and runs any
// custom request initialization.
func (c *OutputService5ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opOutputService5TestCaseOperation1 = "OperationName"

// OutputService5TestCaseOperation1Request generates a request for the OutputService5TestCaseOperation1 operation.
func (c *OutputService5ProtocolTest) OutputService5TestCaseOperation1Request(input *OutputService5TestShapeOutputService5TestCaseOperation1Input) (req *request.Request, output *OutputService5TestShapeOutputService5TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opOutputService5TestCaseOperation1,
	}

	if input == nil {
		input = &OutputService5TestShapeOutputService5TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &OutputService5TestShapeOutputService5TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *OutputService5ProtocolTest) OutputService5TestCaseOperation1(input *OutputService5TestShapeOutputService5TestCaseOperation1Input) (*OutputService5TestShapeOutputService5TestCaseOperation1Output, error) {
	req, out := c.OutputService5TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type OutputService5TestShapeOutputService5TestCaseOperation1Input struct {
	metadataOutputService5TestShapeOutputService5TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataOutputService5TestShapeOutputService5TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService5TestShapeOutputService5TestCaseOperation1Output struct {
	ListMember []*string `type:"list"`

	metadataOutputService5TestShapeOutputService5TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataOutputService5TestShapeOutputService5TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService6ProtocolTest struct {
	*service.Service
}

// New returns a new OutputService6ProtocolTest client.
func NewOutputService6ProtocolTest(config *aws.Config) *OutputService6ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "outputservice6protocoltest",
			APIVersion:   "",
			JSONVersion:  "",
			TargetPrefix: "",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &OutputService6ProtocolTest{service}
}

// newRequest creates a new request for a OutputService6ProtocolTest operation and runs any
// custom request initialization.
func (c *OutputService6ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opOutputService6TestCaseOperation1 = "OperationName"

// OutputService6TestCaseOperation1Request generates a request for the OutputService6TestCaseOperation1 operation.
func (c *OutputService6ProtocolTest) OutputService6TestCaseOperation1Request(input *OutputService6TestShapeOutputService6TestCaseOperation1Input) (req *request.Request, output *OutputService6TestShapeOutputService6TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opOutputService6TestCaseOperation1,
	}

	if input == nil {
		input = &OutputService6TestShapeOutputService6TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &OutputService6TestShapeOutputService6TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *OutputService6ProtocolTest) OutputService6TestCaseOperation1(input *OutputService6TestShapeOutputService6TestCaseOperation1Input) (*OutputService6TestShapeOutputService6TestCaseOperation1Output, error) {
	req, out := c.OutputService6TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type OutputService6TestShapeOutputService6TestCaseOperation1Input struct {
	metadataOutputService6TestShapeOutputService6TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataOutputService6TestShapeOutputService6TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService6TestShapeOutputService6TestCaseOperation1Output struct {
	MapMember map[string][]*int64 `type:"map"`

	metadataOutputService6TestShapeOutputService6TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataOutputService6TestShapeOutputService6TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService7ProtocolTest struct {
	*service.Service
}

// New returns a new OutputService7ProtocolTest client.
func NewOutputService7ProtocolTest(config *aws.Config) *OutputService7ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "outputservice7protocoltest",
			APIVersion:   "",
			JSONVersion:  "",
			TargetPrefix: "",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &OutputService7ProtocolTest{service}
}

// newRequest creates a new request for a OutputService7ProtocolTest operation and runs any
// custom request initialization.
func (c *OutputService7ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opOutputService7TestCaseOperation1 = "OperationName"

// OutputService7TestCaseOperation1Request generates a request for the OutputService7TestCaseOperation1 operation.
func (c *OutputService7ProtocolTest) OutputService7TestCaseOperation1Request(input *OutputService7TestShapeOutputService7TestCaseOperation1Input) (req *request.Request, output *OutputService7TestShapeOutputService7TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opOutputService7TestCaseOperation1,
	}

	if input == nil {
		input = &OutputService7TestShapeOutputService7TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &OutputService7TestShapeOutputService7TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *OutputService7ProtocolTest) OutputService7TestCaseOperation1(input *OutputService7TestShapeOutputService7TestCaseOperation1Input) (*OutputService7TestShapeOutputService7TestCaseOperation1Output, error) {
	req, out := c.OutputService7TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type OutputService7TestShapeOutputService7TestCaseOperation1Input struct {
	metadataOutputService7TestShapeOutputService7TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataOutputService7TestShapeOutputService7TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type OutputService7TestShapeOutputService7TestCaseOperation1Output struct {
	StrType *string `type:"string"`

	metadataOutputService7TestShapeOutputService7TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataOutputService7TestShapeOutputService7TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

//
// Tests begin here
//

func TestOutputService1ProtocolTestEmptyBodyCase1(t *testing.T) {
	svc := NewOutputService1ProtocolTest(nil)

	buf := bytes.NewReader([]byte(""))
	req, out := svc.OutputService1TestCaseOperation1Request(nil)
	req.HTTPResponse = &http.Response{StatusCode: 200, Body: ioutil.NopCloser(buf), Header: http.Header{}}

	// set headers

	// unmarshal response
	jsonrpc.UnmarshalMeta(req)
	jsonrpc.Unmarshal(req)
	assert.NoError(t, req.Error)

	// assert response
	assert.NotNil(t, out) // ensure out variable is used

}

func TestOutputService2ProtocolTestScalarMembersCase1(t *testing.T) {
	svc := NewOutputService2ProtocolTest(nil)

	buf := bytes.NewReader([]byte("{\"Str\": \"myname\", \"Num\": 123, \"FalseBool\": false, \"TrueBool\": true, \"Float\": 1.2, \"Double\": 1.3, \"Long\": 200, \"Char\": \"a\"}"))
	req, out := svc.OutputService2TestCaseOperation1Request(nil)
	req.HTTPResponse = &http.Response{StatusCode: 200, Body: ioutil.NopCloser(buf), Header: http.Header{}}

	// set headers

	// unmarshal response
	jsonrpc.UnmarshalMeta(req)
	jsonrpc.Unmarshal(req)
	assert.NoError(t, req.Error)

	// assert response
	assert.NotNil(t, out) // ensure out variable is used
	assert.Equal(t, "a", *out.Char)
	assert.Equal(t, 1.3, *out.Double)
	assert.Equal(t, false, *out.FalseBool)
	assert.Equal(t, 1.2, *out.Float)
	assert.Equal(t, int64(200), *out.Long)
	assert.Equal(t, int64(123), *out.Num)
	assert.Equal(t, "myname", *out.Str)
	assert.Equal(t, true, *out.TrueBool)

}

func TestOutputService3ProtocolTestBlobMembersCase1(t *testing.T) {
	svc := NewOutputService3ProtocolTest(nil)

	buf := bytes.NewReader([]byte("{\"BlobMember\": \"aGkh\", \"StructMember\": {\"foo\": \"dGhlcmUh\"}}"))
	req, out := svc.OutputService3TestCaseOperation1Request(nil)
	req.HTTPResponse = &http.Response{StatusCode: 200, Body: ioutil.NopCloser(buf), Header: http.Header{}}

	// set headers

	// unmarshal response
	jsonrpc.UnmarshalMeta(req)
	jsonrpc.Unmarshal(req)
	assert.NoError(t, req.Error)

	// assert response
	assert.NotNil(t, out) // ensure out variable is used
	assert.Equal(t, "hi!", string(out.BlobMember))
	assert.Equal(t, "there!", string(out.StructMember.Foo))

}

func TestOutputService4ProtocolTestTimestampMembersCase1(t *testing.T) {
	svc := NewOutputService4ProtocolTest(nil)

	buf := bytes.NewReader([]byte("{\"TimeMember\": 1398796238, \"StructMember\": {\"foo\": 1398796238}}"))
	req, out := svc.OutputService4TestCaseOperation1Request(nil)
	req.HTTPResponse = &http.Response{StatusCode: 200, Body: ioutil.NopCloser(buf), Header: http.Header{}}

	// set headers

	// unmarshal response
	jsonrpc.UnmarshalMeta(req)
	jsonrpc.Unmarshal(req)
	assert.NoError(t, req.Error)

	// assert response
	assert.NotNil(t, out) // ensure out variable is used
	assert.Equal(t, time.Unix(1.398796238e+09, 0).UTC().String(), out.StructMember.Foo.String())
	assert.Equal(t, time.Unix(1.398796238e+09, 0).UTC().String(), out.TimeMember.String())

}

func TestOutputService5ProtocolTestListsCase1(t *testing.T) {
	svc := NewOutputService5ProtocolTest(nil)

	buf := bytes.NewReader([]byte("{\"ListMember\": [\"a\", \"b\"]}"))
	req, out := svc.OutputService5TestCaseOperation1Request(nil)
	req.HTTPResponse = &http.Response{StatusCode: 200, Body: ioutil.NopCloser(buf), Header: http.Header{}}

	// set headers

	// unmarshal response
	jsonrpc.UnmarshalMeta(req)
	jsonrpc.Unmarshal(req)
	assert.NoError(t, req.Error)

	// assert response
	assert.NotNil(t, out) // ensure out variable is used
	assert.Equal(t, "a", *out.ListMember[0])
	assert.Equal(t, "b", *out.ListMember[1])

}

func TestOutputService6ProtocolTestMapsCase1(t *testing.T) {
	svc := NewOutputService6ProtocolTest(nil)

	buf := bytes.NewReader([]byte("{\"MapMember\": {\"a\": [1, 2], \"b\": [3, 4]}}"))
	req, out := svc.OutputService6TestCaseOperation1Request(nil)
	req.HTTPResponse = &http.Response{StatusCode: 200, Body: ioutil.NopCloser(buf), Header: http.Header{}}

	// set headers

	// unmarshal response
	jsonrpc.UnmarshalMeta(req)
	jsonrpc.Unmarshal(req)
	assert.NoError(t, req.Error)

	// assert response
	assert.NotNil(t, out) // ensure out variable is used
	assert.Equal(t, int64(1), *out.MapMember["a"][0])
	assert.Equal(t, int64(2), *out.MapMember["a"][1])
	assert.Equal(t, int64(3), *out.MapMember["b"][0])
	assert.Equal(t, int64(4), *out.MapMember["b"][1])

}

func TestOutputService7ProtocolTestIgnoresExtraDataCase1(t *testing.T) {
	svc := NewOutputService7ProtocolTest(nil)

	buf := bytes.NewReader([]byte("{\"foo\": \"bar\"}"))
	req, out := svc.OutputService7TestCaseOperation1Request(nil)
	req.HTTPResponse = &http.Response{StatusCode: 200, Body: ioutil.NopCloser(buf), Header: http.Header{}}

	// set headers

	// unmarshal response
	jsonrpc.UnmarshalMeta(req)
	jsonrpc.Unmarshal(req)
	assert.NoError(t, req.Error)

	// assert response
	assert.NotNil(t, out) // ensure out variable is used

}
