// Copyright 2020 Dolthub, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeSignedUrl(t *testing.T) {
	res := sanitizeSignedUrl("")
	assert.Equal(t, "", res)

	res = sanitizeSignedUrl("https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCcBe%EXAMPLEKnz3r8O0AgEXAMPLE&Expires=1555531131")
	assert.Equal(t, "https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCc...&Expires=1555531131", res)

	res = sanitizeSignedUrl("https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCcBe%EXAMPLEKnz3r8O0AgEXAMPLE")
	assert.Equal(t, "https://awsexamplebucket.s3.amazonaws.com/test2.txt?AWSAccessKeyId=AKIAEXAMPLEACCESSKEY&Signature=EXHCc...", res)
}
