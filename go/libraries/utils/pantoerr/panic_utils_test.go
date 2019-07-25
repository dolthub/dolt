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

package pantoerr

import (
	"errors"
	"testing"
)

func TestPanicToError(t *testing.T) {
	errMsg := "error message"
	panicMsg := "panic message"
	err := PanicToError(errMsg, func() error {
		panic(panicMsg)
	})

	if err == nil {
		t.Fatal("Should have an error from the panic")
	} else if actualErrMsg := err.Error(); actualErrMsg != errMsg {
		t.Error("Unexpected error message:", actualErrMsg, "does not match expected", errMsg)
	} else if IsRecoveredPanic(err) {
		if GetRecoveredPanicCause(err) != panicMsg {
			t.Error("Unexpected Panic Cause")
		}
	} else {
		t.Error("Recovered panic not of the correct type.")
	}

	errMsg2 := "other error message"
	err = PanicToError(errMsg, func() error {
		return errors.New(errMsg2)
	})

	if err == nil {
		t.Fatal("Should have the error that was returned.")
	} else if err.Error() != errMsg2 {
		t.Error("Unexpected error message")
	}
}

func TestPanicToErrorNil(t *testing.T) {
	errMsg := "error message"
	panicMsg := "panic message"
	err := PanicToErrorNil(errMsg, func() {
		panic(panicMsg)
	})

	if err == nil {
		t.Fatal("Should have an error from the panic")
	} else if actualErrMsg := err.Error(); actualErrMsg != errMsg {
		t.Error("Unexpected error message:", actualErrMsg, "does not match expected", errMsg)
	} else if IsRecoveredPanic(err) {
		if GetRecoveredPanicCause(err) != panicMsg {
			t.Error("Unexpected Panic Cause")
		}
	} else {
		t.Error("Recovered panic not of the correct type.")
	}

	err = PanicToErrorNil(errMsg, func() {
		var i int = 0
		i++
	})

	if err != nil {
		t.Error("Unexpected error message")
	}
}

func TestPanicToErrorInstance(t *testing.T) {
	expected := errors.New("err instance")
	actual := PanicToErrorInstance(expected, func() error {
		panic("panic to err instance")
	})

	if actual != expected {
		t.Fatal("Did not receive expected instance")
	}
}
