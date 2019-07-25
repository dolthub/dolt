// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
