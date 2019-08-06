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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
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

	"github.com/liquidata-inc/dolt/go/store/marshal"
	"github.com/liquidata-inc/dolt/go/store/types"
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
var DateTimeType, _= types.MakeStructTypeFromFields(datetypename, types.FieldMap{
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
	return dateTimeTemplate.NewStruct(vrw.Format(), []types.Value{types.Float(float64(dt.Unix()) + float64(dt.Nanosecond())*1e-9)})
}

// MarshalNomsType makes DateTime implement marshal.TypeMarshaler and it
// allows marshal.MarshalType to work with DateTime.
func (dt DateTime) MarshalNomsType() (*types.Type, error) {
	return DateTimeType, nil
}

// UnmarshalNoms makes DateTime implement marshal.Unmarshaler and it allows
// Noms struct with type DateTimeType able to be unmarshaled onto a DateTime
// Go struct
func (dt *DateTime) UnmarshalNoms(ctx context.Context, nbf *types.NomsBinFormat, v types.Value) error {
	strct := struct {
		SecSinceEpoch float64
	}{}
	err := marshal.Unmarshal(ctx, nbf, v, &strct)
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
	if s, ok := v.(types.Struct); ok {
		if secsV, ok, err := s.MaybeGet("secSinceEpoch"); err != nil {
			panic(err)
		} else if ok {
			if secsF, ok := secsV.(types.Float); ok {
				s, frac := math.Modf(float64(secsF))
				dt := time.Unix(int64(s), int64(frac*1e9))
				return dt.In(c.tz).Format(time.RFC3339)
			}
		}
	}
	return ""
}

func RegisterHRSCommenter(tz *time.Location) {
	hrsCommenter := DateTimeCommenter{tz: tz}
	types.RegisterHRSCommenter(datetypename, hrsEncodingName, hrsCommenter)
}
