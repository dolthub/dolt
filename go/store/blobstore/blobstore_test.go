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

package blobstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

const (
	key        = "test"
	rmwRetries = 5
)

var (
	ctx           context.Context
	bucket        *storage.BucketHandle
	testGCSBucket string
)

func init() {
	testGCSBucket = os.Getenv("TEST_GCS_BUCKET")
	if testGCSBucket != "" {
		ctx = context.Background()
		gcs, err := storage.NewClient(ctx)

		if err != nil {
			panic("Could not create GCSBlobstore")
		}

		bucket = gcs.Bucket(testGCSBucket)
	}
}

type BlobstoreTest struct {
	bsType         string
	bs             Blobstore
	rmwConcurrency int
	rmwIterations  int
}

func appendGCSTest(tests []BlobstoreTest) []BlobstoreTest {
	if testGCSBucket != "" {
		gcsTest := BlobstoreTest{"gcs", &GCSBlobstore{bucket, testGCSBucket, uuid.New().String() + "/"}, 4, 4}
		tests = append(tests, gcsTest)
	}

	return tests
}

func appendLocalTest(tests []BlobstoreTest) []BlobstoreTest {
	dir, err := ioutil.TempDir("", uuid.New().String())

	if err != nil {
		panic("Could not create temp dir")
	}

	return append(tests, BlobstoreTest{"local", NewLocalBlobstore(dir), 10, 20})
}

func newBlobStoreTests() []BlobstoreTest {
	var tests []BlobstoreTest
	tests = append(tests, BlobstoreTest{"inmem", NewInMemoryBlobstore(), 10, 20})
	tests = appendLocalTest(tests)
	tests = appendGCSTest(tests)

	return tests
}

func randBytes(size int) []byte {
	bytes := make([]byte, size)
	rand.Read(bytes)

	return bytes
}

func testPutAndGetBack(t *testing.T, bs Blobstore) {
	testData := randBytes(32)
	ver, err := PutBytes(context.Background(), bs, key, testData)

	if err != nil {
		t.Errorf("Put failed %v.", err)
	}

	retrieved, retVer, err := GetBytes(context.Background(), bs, key, BlobRange{})

	if err != nil {
		t.Errorf("Get failed: %v.", err)
	}

	if ver != retVer {
		t.Errorf("Version doesn't match. Expected: %s Actual: %s.", ver, retVer)
	}

	if !reflect.DeepEqual(retrieved, testData) {
		t.Errorf("Data mismatch.")
	}
}

func TestPutAndGetBack(t *testing.T) {
	for _, bsTest := range newBlobStoreTests() {
		t.Run(bsTest.bsType, func(t *testing.T) {
			testPutAndGetBack(t, bsTest.bs)
		})
	}
}

func testGetMissing(t *testing.T, bs Blobstore) {
	_, _, err := GetBytes(context.Background(), bs, key, BlobRange{})

	if err == nil || !IsNotFoundError(err) {
		t.Errorf("Key should be missing.")
	}
}

func TestGetMissing(t *testing.T) {
	for _, bsTest := range newBlobStoreTests() {
		t.Run(bsTest.bsType, func(t *testing.T) {
			testGetMissing(t, bsTest.bs)
		})
	}
}

// CheckAndPutBytes is a utility method calls bs.CheckAndPut by wrapping the supplied []byte
// in an io.Reader
func CheckAndPutBytes(ctx context.Context, bs Blobstore, expectedVersion, key string, data []byte) (string, error) {
	reader := bytes.NewReader(data)
	return bs.CheckAndPut(ctx, expectedVersion, key, reader)
}

func testCheckAndPutError(t *testing.T, bs Blobstore) {
	testData := randBytes(32)
	badVersion := "bad" //has to be valid hex
	_, err := CheckAndPutBytes(context.Background(), bs, badVersion, key, testData)

	if err == nil {
		t.Errorf("Key should be missing.")
		return
	} else if !IsCheckAndPutError(err) {
		t.Errorf("Should have failed due to version mismatch.")
		return
	}

	cpe, ok := err.(CheckAndPutError)

	if !ok {
		t.Errorf("Error is not of the expected type")
	} else if cpe.Key != key || cpe.ExpectedVersion != badVersion {
		t.Errorf("CheckAndPutError does not have expected values - " + cpe.Error())
	}
}

func TestCheckAndPutError(t *testing.T) {
	for _, bsTest := range newBlobStoreTests() {
		t.Run(bsTest.bsType, func(t *testing.T) {
			testCheckAndPutError(t, bsTest.bs)
		})
	}
}

func testCheckAndPut(t *testing.T, bs Blobstore) {
	ver, err := CheckAndPutBytes(context.Background(), bs, "", key, randBytes(32))

	if err != nil {
		t.Errorf("Failed CheckAndPut.")
	}

	newVer, err := CheckAndPutBytes(context.Background(), bs, ver, key, randBytes(32))

	if err != nil {
		t.Errorf("Failed CheckAndPut.")
	}

	_, err = CheckAndPutBytes(context.Background(), bs, newVer, key, randBytes(32))

	if err != nil {
		t.Errorf("Failed CheckAndPut.")
	}
}

func TestCheckAndPut(t *testing.T) {
	for _, bsTest := range newBlobStoreTests() {
		t.Run(bsTest.bsType, func(t *testing.T) {
			testCheckAndPut(t, bsTest.bs)
		})
	}
}

func readModifyWrite(bs Blobstore, key string, iterations int, doneChan chan int) {
	concurrentWrites := 0
	for updates, failures := 0, 0; updates < iterations; {
		if failures >= rmwRetries {
			panic("Having io issues.")
		}

		data, ver, err := GetBytes(context.Background(), bs, key, BlobRange{})

		if err != nil && !IsNotFoundError(err) {
			log.Println(err)
			failures++
			continue
		}

		dataSize := len(data)
		newData := make([]byte, dataSize+1)
		copy(newData, data)
		newData[dataSize] = byte(dataSize)

		_, err = CheckAndPutBytes(context.Background(), bs, ver, key, newData)

		if err == nil {
			updates++
			failures = 0
		} else if !IsCheckAndPutError(err) {
			log.Println(err)
			failures++
		} else {
			concurrentWrites++
		}
	}

	doneChan <- concurrentWrites
}

func testConcurrentCheckAndPuts(t *testing.T, bsTest BlobstoreTest, key string) {
	doneChan := make(chan int)
	for n := 0; n < bsTest.rmwConcurrency; n++ {
		go readModifyWrite(bsTest.bs, key, bsTest.rmwIterations, doneChan)
	}

	totalConcurrentWrites := 0
	for n := 0; n < bsTest.rmwConcurrency; n++ {
		totalConcurrentWrites += <-doneChan
	}

	// If concurrent writes is 0 this test is pretty shitty
	fmt.Println(totalConcurrentWrites, "concurrent writes occurred")

	var data []byte
	var err error
	for i := 0; i < rmwRetries; i++ {
		data, _, err = GetBytes(context.Background(), bsTest.bs, key, BlobRange{})

		if err == nil {
			break
		}
	}

	if err != nil {
		t.Errorf("Having IO issues testing concurrent blobstore CheckAndPuts")
		return
	}

	if len(data) != bsTest.rmwIterations*bsTest.rmwConcurrency {
		t.Errorf("Output data is not of the correct size. This is caused by bad synchronization where a read/read/write/write has occurred.")
	}

	for i, v := range data {
		if i != int(v) {
			t.Errorf("Data does not match the expected output.")
		}
	}
}

func TestConcurrentCheckAndPuts(t *testing.T) {
	for _, bsTest := range newBlobStoreTests() {
		t.Run(bsTest.bsType, func(t *testing.T) {
			if bsTest.rmwIterations*bsTest.rmwConcurrency > 255 {
				panic("Test epects less than 255 total updates or it won't work as is.")
			}
			testConcurrentCheckAndPuts(t, bsTest, uuid.New().String())
		})
	}
}

func setupRangeTest(t *testing.T, bs Blobstore, data []byte) {
	_, err := PutBytes(context.Background(), bs, key, data)

	if err != nil {
		t.FailNow()
	}
}

func testGetRange(t *testing.T, bs Blobstore, br BlobRange, expected []byte) {
	retrieved, _, err := GetBytes(context.Background(), bs, key, br)

	if err != nil {
		t.Errorf("Get failed: %v.", err)
	}

	if len(retrieved) != len(expected) {
		t.Errorf("Range results are not the right size")
		return
	}

	for i := 0; i < len(expected); i++ {
		if retrieved[i] != expected[i] {
			t.Errorf("Bad Value")
			return
		}
	}
}

func rangeData(min, max int64) []byte {
	if max <= min {
		panic("no")
	}

	size := max - min
	data := make([]byte, 2*size)
	b := bytes.NewBuffer(data[:0])

	for i := int16(min); i < int16(max); i++ {
		binary.Write(b, binary.BigEndian, i)
	}

	return data
}

func TestGetRange(t *testing.T) {
	maxValue := int64(16 * 1024)
	testData := rangeData(0, maxValue)

	tests := newBlobStoreTests()
	for _, bsTest := range tests {
		t.Run(bsTest.bsType, func(t *testing.T) {
			setupRangeTest(t, bsTest.bs, testData)
			// test full range
			testGetRange(t, bsTest.bs, AllRange, rangeData(0, maxValue))
			// test first 2048 bytes (1024 shorts)
			testGetRange(t, bsTest.bs, NewBlobRange(0, 2048), rangeData(0, 1024))

			// test range of values from 1024 to 2048 stored in bytes 2048 to 4096 of the original testData
			testGetRange(t, bsTest.bs, NewBlobRange(2*1024, 2*1024), rangeData(1024, 2048))

			// test the last 2048 bytes of data which will be the last 1024 shorts
			testGetRange(t, bsTest.bs, NewBlobRange(-2*1024, 0), rangeData(maxValue-1024, maxValue))

			// test the range beginning 2048 bytes from the end of size 512 which will be shorts 1024 from the end til 768 from the end
			testGetRange(t, bsTest.bs, NewBlobRange(-2*1024, 512), rangeData(maxValue-1024, maxValue-768))
		})
	}
}

func TestPanicOnNegativeRangeLength(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	NewBlobRange(0, -1)
}
