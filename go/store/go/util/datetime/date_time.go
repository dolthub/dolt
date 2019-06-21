// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// Package datetime implements marshalling of Go DateTime values into Noms structs
// with type DateTimeType.
package datetime

import (
	"context"
	"math"
	"time"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
)

const (
	datetypename    = "DateTime"
	hrsEncodingName = "noms-datetime"
)

// DateTime implements marshaling of time.Time to and from Noms.
type DateTime struct {
	time.Time
}

// DateTimeType is the Noms type used to represent date time objects in Noms.
// The field secSinceEpoch may contain fractions in cases where seconds are
// not sufficient.
var DateTimeType = types.MakeStructTypeFromFields(datetypename, types.FieldMap{
	"secSinceEpoch": types.FloaTType,
})

var dateTimeTemplate = types.MakeStructTemplate(datetypename, []string{"secSinceEpoch"})

// Epoch is the unix Epoch. This time is very consistent,
// which makes it useful for testing or checking for uninitialized values
var Epoch = DateTime{time.Unix(0, 0)}

func init() {
	RegisterHRSCommenter(time.Local)
}

// Now is an alias for a DateTime initialized with time.Now()
func Now() DateTime {
	return DateTime{time.Now()}
}

// MarshalNoms makes DateTime implement marshal.Marshaler and it makes
// DateTime marshal into a Noms struct with type DateTimeType.
func (dt DateTime) MarshalNoms(vrw types.ValueReadWriter) (types.Value, error) {
	return dateTimeTemplate.NewStruct([]types.Value{types.Float(float64(dt.Unix()) + float64(dt.Nanosecond())*1e-9)}), nil
}

// MarshalNomsType makes DateTime implement marshal.TypeMarshaler and it
// allows marshal.MarshalType to work with DateTime.
func (dt DateTime) MarshalNomsType() (*types.Type, error) {
	return DateTimeType, nil
}

// UnmarshalNoms makes DateTime implement marshal.Unmarshaler and it allows
// Noms struct with type DateTimeType able to be unmarshaled onto a DateTime
// Go struct
func (dt *DateTime) UnmarshalNoms(ctx context.Context, v types.Value) error {
	strct := struct {
		SecSinceEpoch float64
	}{}
	err := marshal.Unmarshal(ctx, v, &strct)
	if err != nil {
		return err
	}

	s, frac := math.Modf(strct.SecSinceEpoch)
	*dt = DateTime{time.Unix(int64(s), int64(frac*1e9))}
	return nil
}

type DateTimeCommenter struct {
	tz *time.Location
}

func (c DateTimeCommenter) Comment(ctx context.Context, v types.Value) string {
	if !types.IsValueSubtypeOf(v, DateTimeType) {
		return ""
	}
	var dt DateTime
	marshal.MustUnmarshal(ctx, v, &dt)
	return dt.In(c.tz).Format(time.RFC3339)
}

func RegisterHRSCommenter(tz *time.Location) {
	hrsCommenter := DateTimeCommenter{tz: tz}
	types.RegisterHRSCommenter(datetypename, hrsEncodingName, hrsCommenter)
}
