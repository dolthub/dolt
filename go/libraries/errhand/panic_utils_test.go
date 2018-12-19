package errhand

import (
	"errors"
	"github.com/fatih/color"
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
	} else if rp, ok := err.(*RecoveredPanic); ok {
		if rp.PanicCause.(string) != panicMsg {
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

func TestPanicToVError(t *testing.T) {
	errMsg := "error message"
	panicMsg := "panic message"
	err := PanicToVError(errMsg, func() VerboseError {
		panic(panicMsg)
	})

	if err == nil {
		t.Fatal("Should have an error from the panic")
	} else if actualErrMsg := err.Error(); actualErrMsg != color.RedString(errMsg) {
		t.Errorf(`Unexpected error message:"%s" does not match expected:"%s"`, actualErrMsg, errMsg)
	}

	errMsg2 := "other error message"
	err = PanicToVError(errMsg, func() VerboseError {
		return BuildDError(errMsg2).Build()
	})

	if err == nil {
		t.Fatal("Should have the error that was returned.")
	} else if actualErrMsg := err.Error(); actualErrMsg != color.RedString(errMsg2) {
		t.Errorf(`Unexpected error message:"%s" does not match expected:"%s"`, err.Error(), errMsg)
	}
}
