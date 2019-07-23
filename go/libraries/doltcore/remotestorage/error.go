package remotestorage

import (
	"encoding/json"

	"google.golang.org/grpc/status"
)

type RpcError struct {
	originalErrMsg string
	status         *status.Status
	rpc            string
	host           string
	req            interface{}
}

func NewRpcError(err error, rpc, host string, req interface{}) *RpcError {
	st, _ := status.FromError(err)

	return &RpcError{err.Error(), st, rpc, host, req}
}

func (rpce *RpcError) Error() string {
	return rpce.originalErrMsg
}

func (rpce *RpcError) FullDetails() string {
	jsonStr, _ := GetJsonEncodedRequest(rpce)
	return rpce.originalErrMsg + "\nhost:" + rpce.host + "\nrpc: " + rpce.rpc + "\nparams:" + jsonStr
}

func IsChunkStoreRpcErr(err error) bool {
	_, ok := err.(*RpcError)

	return ok
}

func GetStatus(err error) *status.Status {
	rpce, ok := err.(*RpcError)

	if !ok {
		panic("Bug.  Check IsChunkStoreRpcErr before using this")
	}

	return rpce.status
}

func GetRpc(err error) string {
	rpce, ok := err.(*RpcError)

	if !ok {
		panic("Bug.  Check IsChunkStoreRpcErr before using this")
	}

	return rpce.rpc
}

func GetHost(err error) string {
	rpce, ok := err.(*RpcError)

	if !ok {
		panic("Bug.  Check IsChunkStoreRpcErr before using this")
	}

	return rpce.host
}

func GetRequest(err error) interface{} {
	rpce, ok := err.(*RpcError)

	if !ok {
		panic("Bug.  Check IsChunkStoreRpcErr before using this")
	}

	return rpce.req
}

func GetJsonEncodedRequest(err error) (string, error) {
	rpce, ok := err.(*RpcError)

	if !ok {
		panic("Bug.  Check IsChunkStoreRpcErr before using this")
	}

	data, err := json.MarshalIndent(rpce.req, "", "  ")

	if err != nil {
		return "", err
	}

	return string(data), nil
}
