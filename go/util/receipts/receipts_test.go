// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package receipts

import (
	"math/rand"
	"testing"
	"time"

	"github.com/attic-labs/testify/assert"
)

func TestDecodeKey(t *testing.T) {
	assert := assert.New(t)

	var emptyKey Key

	key, err := DecodeKey("QN8bb2Sj9wp1U7YZ5_O1VYpEVD26YbIFe0b8tw4aW08=")
	assert.NoError(err)
	assert.Equal(Key{
		0x40, 0xdf, 0x1b, 0x6f, 0x64, 0xa3, 0xf7, 0x0a,
		0x75, 0x53, 0xb6, 0x19, 0xe7, 0xf3, 0xb5, 0x55,
		0x8a, 0x44, 0x54, 0x3d, 0xba, 0x61, 0xb2, 0x05,
		0x7b, 0x46, 0xfc, 0xb7, 0x0e, 0x1a, 0x5b, 0x4f,
	}, key)

	key, err = DecodeKey("")
	assert.Error(err)
	assert.Equal(emptyKey, key)

	// Invalid base64.
	key, err = DecodeKey("QN8bb2Sj9wp1U7YZ5_O1VYpEVD26YbIFe0b8tw4aW08")
	assert.Error(err)
	assert.Equal(emptyKey, key)

	// Valid base64, short key.
	key, err = DecodeKey("QN8bb2Sj9wp1U7YZ5_O1VYpEVD26YbIFe0b8tw4a")
	assert.Error(err)
	assert.Equal(emptyKey, key)

	// Valid base64, long key.
	key, err = DecodeKey("QN8bb2Sj9wp1U7YZ5_O1VYpEVD26YbIFe0b8tw4aW088")
	assert.Error(err)
	assert.Equal(emptyKey, key)
}

func TestGenerateValidReceipts(t *testing.T) {
	assert := assert.New(t)

	key := randomKey()
	now := time.Now()

	d := Data{
		Database:  "MyDB",
		IssueDate: now,
	}

	receipt, err := Generate(key, d)
	assert.NoError(err)
	assert.True(receipt != "")

	d2 := Data{
		Database: "MyDB",
	}

	ok, err := Verify(key, receipt, &d2)
	assert.NoError(err)
	assert.True(ok)
	assert.True(now.Equal(d2.IssueDate), "Expected %s, got %s", now, d2.IssueDate)

	d3 := Data{
		Database: "NotMyDB",
	}

	ok, err = Verify(key, receipt, &d3)
	assert.NoError(err)
	assert.False(ok)
	assert.True(now.Equal(d3.IssueDate), "Expected %s, got %s", now, d3.IssueDate)
}

func TestVerifyInvalidReceipt(t *testing.T) {
	assert := assert.New(t)

	key := randomKey()
	d := Data{
		Database: "MyDB",
	}

	ok, err := Verify(key, "foobar", &d)
	assert.Error(err)
	assert.False(ok)
	assert.True((time.Time{}).Equal(d.IssueDate))
}

func TestReceiptsAreUnique(t *testing.T) {
	assert := assert.New(t)

	key := randomKey()
	d := Data{
		Database:  "MyDB",
		IssueDate: time.Now(),
	}

	r1, err := Generate(key, d)
	assert.NoError(err)
	r2, err := Generate(key, d)
	assert.NoError(err)
	r3, err := Generate(key, d)
	assert.NoError(err)

	assert.NotEqual(r1, r2)
	assert.NotEqual(r1, r3)
	assert.NotEqual(r2, r3)

	assert.Equal(len(r1), len(r2))
	assert.Equal(len(r1), len(r3))
	assert.Equal(len(r2), len(r3))
}

func randomKey() (key Key) {
	rand.Read(key[:])
	return
}
