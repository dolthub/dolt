// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package d implements several debug, error and assertion functions used throughout Noms.
package d

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/stretchr/testify/assert"
)

// d.Chk.<Method>() -- used in test cases and as assertions
var (
	Chk = assert.New(&panicker{})
)

type panicker struct {
}

func (s panicker) Errorf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

// Panic(err) creates an error using format and args and wraps it in a
// WrappedError which can be handled using Try() and TryCatch()
func Panic(format string, args ...interface{}) {

	if len(args) == 0 {
		err := errors.New(format)
		panic(Wrap(err))
	}
	err := fmt.Errorf(format, args...)
	panic(Wrap(err))
}

// PanicIfError(err) && PanicIfTrue(expr) can be used to panic in a way that's
// easily handled by Try() and TryCatch()
func PanicIfError(err error) {
	if err != nil {
		panic(Wrap(err))
	}
}

// If b is true, creates a default error, wraps it and panics.
func PanicIfTrue(b bool) {
	if b {
		panic(Wrap(errors.New("Expected true")))
	}
}

// If b is false, creates a default error, wraps it and panics.
func PanicIfFalse(b bool) {
	if !b {
		panic(Wrap(errors.New("Expected false")))
	}
}

// If 'f' panics with a WrappedError then recover that error.
// If types is empty, return the WrappedError.
// if types is not empty and cause is not one of the listed types, re-panic.
// if types is not empty and cause is one of the types, return 'cause'
func Try(f func(), types ...interface{}) (err error) {
	defer recoverWrappedTypes(&err, types)
	f()
	return
}

// If 'f' panics with a WrappedError then recover that error and return it.
// If types is empty, return the WrappedError.
// if types is not empty and cause is not one of the listed types, re-panic.
// if types is not empty and cause is one of the types, return 'cause'
func TryCatch(f func(), catch func(err error) error) (err error) {
	defer recoverWrapped(&err, catch)
	f()
	return
}

type WrappedError interface {
	Error() string
	Cause() error
}

// Wraps an error. The enclosing error has a default Error() that contains the error msg along
// with a backtrace. The original error can be retrieved by calling err.Cause().
func Wrap(err error) WrappedError {
	if err == nil {
		return nil
	}
	if we, ok := err.(WrappedError); ok {
		return we
	}

	st := stackTracer{}
	assert := assert.New(&st)
	assert.Fail(err.Error())

	return wrappedError{st.stackTrace, err}
}

// If err is a WrappedError, then Cause() is returned, otherwise returns err.
func Unwrap(err error) error {
	cause := err
	we, ok := err.(WrappedError)
	if ok {
		cause = we.Cause()
	}
	return cause
}

func causeInTypes(err error, types ...interface{}) bool {
	cause := Unwrap(err)
	typ := reflect.TypeOf(cause)
	for _, t := range types {
		if typ == reflect.TypeOf(t) {
			return true
		}
	}
	return false
}

// Utility method, that checks type of error and panics with wrapped error not one of the listed types.
func PanicIfNotType(err error, types ...interface{}) error {
	if err == nil {
		return nil
	}
	if !causeInTypes(err, types...) {
		we, ok := err.(WrappedError)
		if !ok {
			we = Wrap(err)
		}
		panic(we)
	}
	return Unwrap(err)
}

type wrappedError struct {
	msg   string
	cause error
}

func (we wrappedError) Error() string { return we.msg }
func (we wrappedError) Cause() error  { return we.cause }

type stackTracer struct {
	stackTrace string
}

func (s *stackTracer) Errorf(format string, args ...interface{}) {
	s.stackTrace = fmt.Sprintf(format, args...)
}

func recoverWrappedTypes(errp *error, types []interface{}) {
	if r := recover(); r != nil {
		if wrapper, ok := r.(wrappedError); !ok {
			panic(r)
		} else if len(types) > 0 && !causeInTypes(wrapper, types...) {
			panic(r)
		} else if len(types) > 0 {
			*errp = wrapper.Cause()
		} else {
			*errp = wrapper
		}
	}
}

func recoverWrapped(errp *error, catch func(err error) error) {
	if r := recover(); r != nil {
		we, ok := r.(wrappedError)
		if !ok {
			panic(r)
		}
		if catch != nil {
			*errp = catch(we)
		} else {
			*errp = Unwrap(we)
		}
	}
}
