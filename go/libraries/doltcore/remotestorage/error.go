// Copyright 2019 Dolthub, Inc.
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
	"log"
	"runtime"
	"strings"
	"time"

	"google.golang.org/grpc/status"
)

type RpcError struct {
	originalErrMsg string
	status         *status.Status
	rpc            string
	host           string
	req            interface{}
	timestamp      time.Time
	stackTrace     string
}

func NewRpcError(err error, rpc, host string, req interface{}) *RpcError {
	st, _ := status.FromError(err)
	
	// Capture stack trace
	buf := make([]byte, 1024*4)
	n := runtime.Stack(buf, false)
	stackLines := strings.Split(string(buf[:n]), "\n")
	var stackTrace string
	// Skip first few lines (runtime.Stack, NewRpcError)
	if len(stackLines) > 4 {
		stackTrace = strings.Join(stackLines[4:], "\n")
	} else {
		stackTrace = string(buf[:n])
	}

	rpcErr := &RpcError{err.Error(), st, rpc, host, req, time.Now(), stackTrace}
	
	// DEBUG: Log RPC error creation
	log.Printf("DEBUG: Creating RpcError:")
	log.Printf("  Error: %s", err.Error())
	log.Printf("  RPC: %s", rpc)
	log.Printf("  Host: %s", host)
	log.Printf("  Timestamp: %s", rpcErr.timestamp.Format(time.RFC3339Nano))
	if st != nil {
		log.Printf("  GRPC Status Code: %s", st.Code().String())
		log.Printf("  GRPC Status Message: %s", st.Message())
	}
	
	return rpcErr
}

func (rpce *RpcError) Error() string {
	return rpce.originalErrMsg
}

func (rpce *RpcError) IsPermanent() bool {
	return statusCodeIsPermanentError(rpce.status)
}

func (rpce *RpcError) FullDetails() string {
	jsonStr, _ := GetJsonEncodedRequest(rpce)
	details := rpce.originalErrMsg + "\nhost: " + rpce.host + "\nrpc: " + rpce.rpc + "\ntimestamp: " + rpce.timestamp.Format(time.RFC3339Nano) + "\nparams: " + jsonStr
	
	if rpce.status != nil {
		details += "\ngrpc_status_code: " + rpce.status.Code().String()
		details += "\ngrpc_status_message: " + rpce.status.Message()
	}
	
	if rpce.stackTrace != "" {
		details += "\nstack_trace:\n" + rpce.stackTrace
	}
	
	return details
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
