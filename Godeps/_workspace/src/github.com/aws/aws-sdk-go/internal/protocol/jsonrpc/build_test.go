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

type InputService1ProtocolTest struct {
	*service.Service
}

// New returns a new InputService1ProtocolTest client.
func NewInputService1ProtocolTest(config *aws.Config) *InputService1ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "inputservice1protocoltest",
			APIVersion:   "",
			JSONVersion:  "1.1",
			TargetPrefix: "com.amazonaws.foo",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &InputService1ProtocolTest{service}
}

// newRequest creates a new request for a InputService1ProtocolTest operation and runs any
// custom request initialization.
func (c *InputService1ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opInputService1TestCaseOperation1 = "OperationName"

// InputService1TestCaseOperation1Request generates a request for the InputService1TestCaseOperation1 operation.
func (c *InputService1ProtocolTest) InputService1TestCaseOperation1Request(input *InputService1TestShapeInputService1TestCaseOperation1Input) (req *request.Request, output *InputService1TestShapeInputService1TestCaseOperation1Output) {
	op := &request.Operation{
		Name:       opInputService1TestCaseOperation1,
		HTTPMethod: "POST",
	}

	if input == nil {
		input = &InputService1TestShapeInputService1TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService1TestShapeInputService1TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *InputService1ProtocolTest) InputService1TestCaseOperation1(input *InputService1TestShapeInputService1TestCaseOperation1Input) (*InputService1TestShapeInputService1TestCaseOperation1Output, error) {
	req, out := c.InputService1TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type InputService1TestShapeInputService1TestCaseOperation1Input struct {
	Name *string `type:"string"`

	metadataInputService1TestShapeInputService1TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataInputService1TestShapeInputService1TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService1TestShapeInputService1TestCaseOperation1Output struct {
	metadataInputService1TestShapeInputService1TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataInputService1TestShapeInputService1TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService2ProtocolTest struct {
	*service.Service
}

// New returns a new InputService2ProtocolTest client.
func NewInputService2ProtocolTest(config *aws.Config) *InputService2ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "inputservice2protocoltest",
			APIVersion:   "",
			JSONVersion:  "1.1",
			TargetPrefix: "com.amazonaws.foo",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &InputService2ProtocolTest{service}
}

// newRequest creates a new request for a InputService2ProtocolTest operation and runs any
// custom request initialization.
func (c *InputService2ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opInputService2TestCaseOperation1 = "OperationName"

// InputService2TestCaseOperation1Request generates a request for the InputService2TestCaseOperation1 operation.
func (c *InputService2ProtocolTest) InputService2TestCaseOperation1Request(input *InputService2TestShapeInputService2TestCaseOperation1Input) (req *request.Request, output *InputService2TestShapeInputService2TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opInputService2TestCaseOperation1,
	}

	if input == nil {
		input = &InputService2TestShapeInputService2TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService2TestShapeInputService2TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *InputService2ProtocolTest) InputService2TestCaseOperation1(input *InputService2TestShapeInputService2TestCaseOperation1Input) (*InputService2TestShapeInputService2TestCaseOperation1Output, error) {
	req, out := c.InputService2TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type InputService2TestShapeInputService2TestCaseOperation1Input struct {
	TimeArg *time.Time `type:"timestamp" timestampFormat:"unix"`

	metadataInputService2TestShapeInputService2TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataInputService2TestShapeInputService2TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService2TestShapeInputService2TestCaseOperation1Output struct {
	metadataInputService2TestShapeInputService2TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataInputService2TestShapeInputService2TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService3ProtocolTest struct {
	*service.Service
}

// New returns a new InputService3ProtocolTest client.
func NewInputService3ProtocolTest(config *aws.Config) *InputService3ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "inputservice3protocoltest",
			APIVersion:   "",
			JSONVersion:  "1.1",
			TargetPrefix: "com.amazonaws.foo",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &InputService3ProtocolTest{service}
}

// newRequest creates a new request for a InputService3ProtocolTest operation and runs any
// custom request initialization.
func (c *InputService3ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opInputService3TestCaseOperation1 = "OperationName"

// InputService3TestCaseOperation1Request generates a request for the InputService3TestCaseOperation1 operation.
func (c *InputService3ProtocolTest) InputService3TestCaseOperation1Request(input *InputService3TestShapeInputShape) (req *request.Request, output *InputService3TestShapeInputService3TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opInputService3TestCaseOperation1,
	}

	if input == nil {
		input = &InputService3TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService3TestShapeInputService3TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *InputService3ProtocolTest) InputService3TestCaseOperation1(input *InputService3TestShapeInputShape) (*InputService3TestShapeInputService3TestCaseOperation1Output, error) {
	req, out := c.InputService3TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

const opInputService3TestCaseOperation2 = "OperationName"

// InputService3TestCaseOperation2Request generates a request for the InputService3TestCaseOperation2 operation.
func (c *InputService3ProtocolTest) InputService3TestCaseOperation2Request(input *InputService3TestShapeInputShape) (req *request.Request, output *InputService3TestShapeInputService3TestCaseOperation2Output) {
	op := &request.Operation{
		Name: opInputService3TestCaseOperation2,
	}

	if input == nil {
		input = &InputService3TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService3TestShapeInputService3TestCaseOperation2Output{}
	req.Data = output
	return
}

func (c *InputService3ProtocolTest) InputService3TestCaseOperation2(input *InputService3TestShapeInputShape) (*InputService3TestShapeInputService3TestCaseOperation2Output, error) {
	req, out := c.InputService3TestCaseOperation2Request(input)
	err := req.Send()
	return out, err
}

type InputService3TestShapeInputService3TestCaseOperation1Output struct {
	metadataInputService3TestShapeInputService3TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataInputService3TestShapeInputService3TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService3TestShapeInputService3TestCaseOperation2Output struct {
	metadataInputService3TestShapeInputService3TestCaseOperation2Output `json:"-" xml:"-"`
}

type metadataInputService3TestShapeInputService3TestCaseOperation2Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService3TestShapeInputShape struct {
	BlobArg []byte `type:"blob"`

	BlobMap map[string][]byte `type:"map"`

	metadataInputService3TestShapeInputShape `json:"-" xml:"-"`
}

type metadataInputService3TestShapeInputShape struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService4ProtocolTest struct {
	*service.Service
}

// New returns a new InputService4ProtocolTest client.
func NewInputService4ProtocolTest(config *aws.Config) *InputService4ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "inputservice4protocoltest",
			APIVersion:   "",
			JSONVersion:  "1.1",
			TargetPrefix: "com.amazonaws.foo",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &InputService4ProtocolTest{service}
}

// newRequest creates a new request for a InputService4ProtocolTest operation and runs any
// custom request initialization.
func (c *InputService4ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opInputService4TestCaseOperation1 = "OperationName"

// InputService4TestCaseOperation1Request generates a request for the InputService4TestCaseOperation1 operation.
func (c *InputService4ProtocolTest) InputService4TestCaseOperation1Request(input *InputService4TestShapeInputService4TestCaseOperation1Input) (req *request.Request, output *InputService4TestShapeInputService4TestCaseOperation1Output) {
	op := &request.Operation{
		Name:       opInputService4TestCaseOperation1,
		HTTPMethod: "POST",
	}

	if input == nil {
		input = &InputService4TestShapeInputService4TestCaseOperation1Input{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService4TestShapeInputService4TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *InputService4ProtocolTest) InputService4TestCaseOperation1(input *InputService4TestShapeInputService4TestCaseOperation1Input) (*InputService4TestShapeInputService4TestCaseOperation1Output, error) {
	req, out := c.InputService4TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

type InputService4TestShapeInputService4TestCaseOperation1Input struct {
	ListParam [][]byte `type:"list"`

	metadataInputService4TestShapeInputService4TestCaseOperation1Input `json:"-" xml:"-"`
}

type metadataInputService4TestShapeInputService4TestCaseOperation1Input struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService4TestShapeInputService4TestCaseOperation1Output struct {
	metadataInputService4TestShapeInputService4TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataInputService4TestShapeInputService4TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5ProtocolTest struct {
	*service.Service
}

// New returns a new InputService5ProtocolTest client.
func NewInputService5ProtocolTest(config *aws.Config) *InputService5ProtocolTest {
	service := &service.Service{
		ServiceInfo: serviceinfo.ServiceInfo{
			Config:       defaults.DefaultConfig.Merge(config),
			ServiceName:  "inputservice5protocoltest",
			APIVersion:   "",
			JSONVersion:  "1.1",
			TargetPrefix: "com.amazonaws.foo",
		},
	}
	service.Initialize()

	// Handlers
	service.Handlers.Sign.PushBack(v4.Sign)
	service.Handlers.Build.PushBack(jsonrpc.Build)
	service.Handlers.Unmarshal.PushBack(jsonrpc.Unmarshal)
	service.Handlers.UnmarshalMeta.PushBack(jsonrpc.UnmarshalMeta)
	service.Handlers.UnmarshalError.PushBack(jsonrpc.UnmarshalError)

	return &InputService5ProtocolTest{service}
}

// newRequest creates a new request for a InputService5ProtocolTest operation and runs any
// custom request initialization.
func (c *InputService5ProtocolTest) newRequest(op *request.Operation, params, data interface{}) *request.Request {
	req := c.NewRequest(op, params, data)

	return req
}

const opInputService5TestCaseOperation1 = "OperationName"

// InputService5TestCaseOperation1Request generates a request for the InputService5TestCaseOperation1 operation.
func (c *InputService5ProtocolTest) InputService5TestCaseOperation1Request(input *InputService5TestShapeInputShape) (req *request.Request, output *InputService5TestShapeInputService5TestCaseOperation1Output) {
	op := &request.Operation{
		Name: opInputService5TestCaseOperation1,
	}

	if input == nil {
		input = &InputService5TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService5TestShapeInputService5TestCaseOperation1Output{}
	req.Data = output
	return
}

func (c *InputService5ProtocolTest) InputService5TestCaseOperation1(input *InputService5TestShapeInputShape) (*InputService5TestShapeInputService5TestCaseOperation1Output, error) {
	req, out := c.InputService5TestCaseOperation1Request(input)
	err := req.Send()
	return out, err
}

const opInputService5TestCaseOperation2 = "OperationName"

// InputService5TestCaseOperation2Request generates a request for the InputService5TestCaseOperation2 operation.
func (c *InputService5ProtocolTest) InputService5TestCaseOperation2Request(input *InputService5TestShapeInputShape) (req *request.Request, output *InputService5TestShapeInputService5TestCaseOperation2Output) {
	op := &request.Operation{
		Name: opInputService5TestCaseOperation2,
	}

	if input == nil {
		input = &InputService5TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService5TestShapeInputService5TestCaseOperation2Output{}
	req.Data = output
	return
}

func (c *InputService5ProtocolTest) InputService5TestCaseOperation2(input *InputService5TestShapeInputShape) (*InputService5TestShapeInputService5TestCaseOperation2Output, error) {
	req, out := c.InputService5TestCaseOperation2Request(input)
	err := req.Send()
	return out, err
}

const opInputService5TestCaseOperation3 = "OperationName"

// InputService5TestCaseOperation3Request generates a request for the InputService5TestCaseOperation3 operation.
func (c *InputService5ProtocolTest) InputService5TestCaseOperation3Request(input *InputService5TestShapeInputShape) (req *request.Request, output *InputService5TestShapeInputService5TestCaseOperation3Output) {
	op := &request.Operation{
		Name: opInputService5TestCaseOperation3,
	}

	if input == nil {
		input = &InputService5TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService5TestShapeInputService5TestCaseOperation3Output{}
	req.Data = output
	return
}

func (c *InputService5ProtocolTest) InputService5TestCaseOperation3(input *InputService5TestShapeInputShape) (*InputService5TestShapeInputService5TestCaseOperation3Output, error) {
	req, out := c.InputService5TestCaseOperation3Request(input)
	err := req.Send()
	return out, err
}

const opInputService5TestCaseOperation4 = "OperationName"

// InputService5TestCaseOperation4Request generates a request for the InputService5TestCaseOperation4 operation.
func (c *InputService5ProtocolTest) InputService5TestCaseOperation4Request(input *InputService5TestShapeInputShape) (req *request.Request, output *InputService5TestShapeInputService5TestCaseOperation4Output) {
	op := &request.Operation{
		Name: opInputService5TestCaseOperation4,
	}

	if input == nil {
		input = &InputService5TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService5TestShapeInputService5TestCaseOperation4Output{}
	req.Data = output
	return
}

func (c *InputService5ProtocolTest) InputService5TestCaseOperation4(input *InputService5TestShapeInputShape) (*InputService5TestShapeInputService5TestCaseOperation4Output, error) {
	req, out := c.InputService5TestCaseOperation4Request(input)
	err := req.Send()
	return out, err
}

const opInputService5TestCaseOperation5 = "OperationName"

// InputService5TestCaseOperation5Request generates a request for the InputService5TestCaseOperation5 operation.
func (c *InputService5ProtocolTest) InputService5TestCaseOperation5Request(input *InputService5TestShapeInputShape) (req *request.Request, output *InputService5TestShapeInputService5TestCaseOperation5Output) {
	op := &request.Operation{
		Name: opInputService5TestCaseOperation5,
	}

	if input == nil {
		input = &InputService5TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService5TestShapeInputService5TestCaseOperation5Output{}
	req.Data = output
	return
}

func (c *InputService5ProtocolTest) InputService5TestCaseOperation5(input *InputService5TestShapeInputShape) (*InputService5TestShapeInputService5TestCaseOperation5Output, error) {
	req, out := c.InputService5TestCaseOperation5Request(input)
	err := req.Send()
	return out, err
}

const opInputService5TestCaseOperation6 = "OperationName"

// InputService5TestCaseOperation6Request generates a request for the InputService5TestCaseOperation6 operation.
func (c *InputService5ProtocolTest) InputService5TestCaseOperation6Request(input *InputService5TestShapeInputShape) (req *request.Request, output *InputService5TestShapeInputService5TestCaseOperation6Output) {
	op := &request.Operation{
		Name: opInputService5TestCaseOperation6,
	}

	if input == nil {
		input = &InputService5TestShapeInputShape{}
	}

	req = c.newRequest(op, input, output)
	output = &InputService5TestShapeInputService5TestCaseOperation6Output{}
	req.Data = output
	return
}

func (c *InputService5ProtocolTest) InputService5TestCaseOperation6(input *InputService5TestShapeInputShape) (*InputService5TestShapeInputService5TestCaseOperation6Output, error) {
	req, out := c.InputService5TestCaseOperation6Request(input)
	err := req.Send()
	return out, err
}

type InputService5TestShapeInputService5TestCaseOperation1Output struct {
	metadataInputService5TestShapeInputService5TestCaseOperation1Output `json:"-" xml:"-"`
}

type metadataInputService5TestShapeInputService5TestCaseOperation1Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5TestShapeInputService5TestCaseOperation2Output struct {
	metadataInputService5TestShapeInputService5TestCaseOperation2Output `json:"-" xml:"-"`
}

type metadataInputService5TestShapeInputService5TestCaseOperation2Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5TestShapeInputService5TestCaseOperation3Output struct {
	metadataInputService5TestShapeInputService5TestCaseOperation3Output `json:"-" xml:"-"`
}

type metadataInputService5TestShapeInputService5TestCaseOperation3Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5TestShapeInputService5TestCaseOperation4Output struct {
	metadataInputService5TestShapeInputService5TestCaseOperation4Output `json:"-" xml:"-"`
}

type metadataInputService5TestShapeInputService5TestCaseOperation4Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5TestShapeInputService5TestCaseOperation5Output struct {
	metadataInputService5TestShapeInputService5TestCaseOperation5Output `json:"-" xml:"-"`
}

type metadataInputService5TestShapeInputService5TestCaseOperation5Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5TestShapeInputService5TestCaseOperation6Output struct {
	metadataInputService5TestShapeInputService5TestCaseOperation6Output `json:"-" xml:"-"`
}

type metadataInputService5TestShapeInputService5TestCaseOperation6Output struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5TestShapeInputShape struct {
	RecursiveStruct *InputService5TestShapeRecursiveStructType `type:"structure"`

	metadataInputService5TestShapeInputShape `json:"-" xml:"-"`
}

type metadataInputService5TestShapeInputShape struct {
	SDKShapeTraits bool `type:"structure"`
}

type InputService5TestShapeRecursiveStructType struct {
	NoRecurse *string `type:"string"`

	RecursiveList []*InputService5TestShapeRecursiveStructType `type:"list"`

	RecursiveMap map[string]*InputService5TestShapeRecursiveStructType `type:"map"`

	RecursiveStruct *InputService5TestShapeRecursiveStructType `type:"structure"`

	metadataInputService5TestShapeRecursiveStructType `json:"-" xml:"-"`
}

type metadataInputService5TestShapeRecursiveStructType struct {
	SDKShapeTraits bool `type:"structure"`
}

//
// Tests begin here
//

func TestInputService1ProtocolTestScalarMembersCase1(t *testing.T) {
	svc := NewInputService1ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService1TestShapeInputService1TestCaseOperation1Input{
		Name: aws.String("myname"),
	}
	req, _ := svc.InputService1TestCaseOperation1Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"Name":"myname"}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService2ProtocolTestTimestampValuesCase1(t *testing.T) {
	svc := NewInputService2ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService2TestShapeInputService2TestCaseOperation1Input{
		TimeArg: aws.Time(time.Unix(1422172800, 0)),
	}
	req, _ := svc.InputService2TestCaseOperation1Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"TimeArg":1422172800}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService3ProtocolTestBase64EncodedBlobsCase1(t *testing.T) {
	svc := NewInputService3ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService3TestShapeInputShape{
		BlobArg: []byte("foo"),
	}
	req, _ := svc.InputService3TestCaseOperation1Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"BlobArg":"Zm9v"}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService3ProtocolTestBase64EncodedBlobsCase2(t *testing.T) {
	svc := NewInputService3ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService3TestShapeInputShape{
		BlobMap: map[string][]byte{
			"key1": []byte("foo"),
			"key2": []byte("bar"),
		},
	}
	req, _ := svc.InputService3TestCaseOperation2Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"BlobMap":{"key1":"Zm9v","key2":"YmFy"}}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService4ProtocolTestNestedBlobsCase1(t *testing.T) {
	svc := NewInputService4ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService4TestShapeInputService4TestCaseOperation1Input{
		ListParam: [][]byte{
			[]byte("foo"),
			[]byte("bar"),
		},
	}
	req, _ := svc.InputService4TestCaseOperation1Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"ListParam":["Zm9v","YmFy"]}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService5ProtocolTestRecursiveShapesCase1(t *testing.T) {
	svc := NewInputService5ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService5TestShapeInputShape{
		RecursiveStruct: &InputService5TestShapeRecursiveStructType{
			NoRecurse: aws.String("foo"),
		},
	}
	req, _ := svc.InputService5TestCaseOperation1Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"RecursiveStruct":{"NoRecurse":"foo"}}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService5ProtocolTestRecursiveShapesCase2(t *testing.T) {
	svc := NewInputService5ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService5TestShapeInputShape{
		RecursiveStruct: &InputService5TestShapeRecursiveStructType{
			RecursiveStruct: &InputService5TestShapeRecursiveStructType{
				NoRecurse: aws.String("foo"),
			},
		},
	}
	req, _ := svc.InputService5TestCaseOperation2Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"RecursiveStruct":{"RecursiveStruct":{"NoRecurse":"foo"}}}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService5ProtocolTestRecursiveShapesCase3(t *testing.T) {
	svc := NewInputService5ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService5TestShapeInputShape{
		RecursiveStruct: &InputService5TestShapeRecursiveStructType{
			RecursiveStruct: &InputService5TestShapeRecursiveStructType{
				RecursiveStruct: &InputService5TestShapeRecursiveStructType{
					RecursiveStruct: &InputService5TestShapeRecursiveStructType{
						NoRecurse: aws.String("foo"),
					},
				},
			},
		},
	}
	req, _ := svc.InputService5TestCaseOperation3Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"RecursiveStruct":{"RecursiveStruct":{"RecursiveStruct":{"RecursiveStruct":{"NoRecurse":"foo"}}}}}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService5ProtocolTestRecursiveShapesCase4(t *testing.T) {
	svc := NewInputService5ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService5TestShapeInputShape{
		RecursiveStruct: &InputService5TestShapeRecursiveStructType{
			RecursiveList: []*InputService5TestShapeRecursiveStructType{
				{
					NoRecurse: aws.String("foo"),
				},
				{
					NoRecurse: aws.String("bar"),
				},
			},
		},
	}
	req, _ := svc.InputService5TestCaseOperation4Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"RecursiveStruct":{"RecursiveList":[{"NoRecurse":"foo"},{"NoRecurse":"bar"}]}}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService5ProtocolTestRecursiveShapesCase5(t *testing.T) {
	svc := NewInputService5ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService5TestShapeInputShape{
		RecursiveStruct: &InputService5TestShapeRecursiveStructType{
			RecursiveList: []*InputService5TestShapeRecursiveStructType{
				{
					NoRecurse: aws.String("foo"),
				},
				{
					RecursiveStruct: &InputService5TestShapeRecursiveStructType{
						NoRecurse: aws.String("bar"),
					},
				},
			},
		},
	}
	req, _ := svc.InputService5TestCaseOperation5Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"RecursiveStruct":{"RecursiveList":[{"NoRecurse":"foo"},{"RecursiveStruct":{"NoRecurse":"bar"}}]}}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}

func TestInputService5ProtocolTestRecursiveShapesCase6(t *testing.T) {
	svc := NewInputService5ProtocolTest(nil)
	svc.Endpoint = "https://test"

	input := &InputService5TestShapeInputShape{
		RecursiveStruct: &InputService5TestShapeRecursiveStructType{
			RecursiveMap: map[string]*InputService5TestShapeRecursiveStructType{
				"bar": {
					NoRecurse: aws.String("bar"),
				},
				"foo": {
					NoRecurse: aws.String("foo"),
				},
			},
		},
	}
	req, _ := svc.InputService5TestCaseOperation6Request(input)
	r := req.HTTPRequest

	// build request
	jsonrpc.Build(req)
	assert.NoError(t, req.Error)

	// assert body
	assert.NotNil(t, r.Body)
	body, _ := ioutil.ReadAll(r.Body)
	assert.Equal(t, util.Trim(`{"RecursiveStruct":{"RecursiveMap":{"bar":{"NoRecurse":"bar"},"foo":{"NoRecurse":"foo"}}}}`), util.Trim(string(body)))

	// assert URL
	assert.Equal(t, "https://test/", r.URL.String())

	// assert headers
	assert.Equal(t, "application/x-amz-json-1.1", r.Header.Get("Content-Type"))
	assert.Equal(t, "com.amazonaws.foo.OperationName", r.Header.Get("X-Amz-Target"))

}
