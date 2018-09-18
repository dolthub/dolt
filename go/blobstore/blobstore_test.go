package blobstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"reflect"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

const (
	key           = "test"
	rmwRetries    = 5
	testGCSBucket = ""
)

var (
	ctx    context.Context
	bucket *storage.BucketHandle
)

func init() {
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
	bs             Blobstore
	rmwConcurrency int
	rmwIterations  int
}

func appendGCSTest(tests []BlobstoreTest) []BlobstoreTest {
	if testGCSBucket != "" {
		gcsTest := BlobstoreTest{&GCSBlobstore{ctx, bucket, uuid.New().String() + "/"}, 4, 4}
		tests = append(tests, gcsTest)
	}

	return tests
}

func appendLocalTest(tests []BlobstoreTest) []BlobstoreTest {
	dir, err := ioutil.TempDir("", uuid.New().String())

	if err != nil {
		panic("Could not create temp dir")
	}

	return append(tests, BlobstoreTest{NewLocalBlobstore(dir), 10, 20})
}

func newBlobStoreTests() []BlobstoreTest {
	var tests []BlobstoreTest
	tests = append(tests, BlobstoreTest{NewInMemoryBlobstore(), 10, 20})
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
	ver, err := PutBytes(bs, key, testData)

	if err != nil {
		t.Errorf("Put failed %v.", err)
	}

	retrieved, retVer, err := GetBytes(bs, key, BlobRange{})

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
		testPutAndGetBack(t, bsTest.bs)
	}
}

func testGetMissing(t *testing.T, bs Blobstore) {
	_, _, err := GetBytes(bs, key, BlobRange{})

	if err == nil || !IsNotFoundError(err) {
		t.Errorf("Key should be missing.")
	}
}

func TestGetMissing(t *testing.T) {
	for _, bsTest := range newBlobStoreTests() {
		testGetMissing(t, bsTest.bs)
	}
}

func testCheckAndPutError(t *testing.T, bs Blobstore) {
	testData := randBytes(32)
	badVersion := "bad" //has to be valid hex
	_, err := CheckAndPutBytes(bs, badVersion, key, testData)

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
		testCheckAndPutError(t, bsTest.bs)
	}
}

func testCheckAndPut(t *testing.T, bs Blobstore) {
	ver, err := CheckAndPutBytes(bs, "", key, randBytes(32))

	if err != nil {
		t.Errorf("Failed CheckAndPut.")
	}

	newVer, err := CheckAndPutBytes(bs, ver, key, randBytes(32))

	if err != nil {
		t.Errorf("Failed CheckAndPut.")
	}

	_, err = CheckAndPutBytes(bs, newVer, key, randBytes(32))

	if err != nil {
		t.Errorf("Failed CheckAndPut.")
	}
}

func TestCheckAndPut(t *testing.T) {
	for _, bsTest := range newBlobStoreTests() {
		testCheckAndPut(t, bsTest.bs)
	}
}

func readModifyWrite(bs Blobstore, key string, iterations int, doneChan chan int) {
	concurrentWrites := 0
	for updates, failures := 0, 0; updates < iterations; {
		if failures >= rmwRetries {
			panic("Having io issues.")
		}

		data, ver, err := GetBytes(bs, key, BlobRange{})

		if err != nil && !IsNotFoundError(err) {
			log.Println(err)
			failures++
			continue
		}

		dataSize := len(data)
		newData := make([]byte, dataSize+1)
		copy(newData, data)
		newData[dataSize] = byte(dataSize)

		_, err = CheckAndPutBytes(bs, ver, key, newData)

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
		data, _, err = GetBytes(bsTest.bs, key, BlobRange{})

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
		if bsTest.rmwIterations*bsTest.rmwConcurrency > 255 {
			panic("Test epects less than 255 total updates or it won't work as is.")
		}
		testConcurrentCheckAndPuts(t, bsTest, uuid.New().String())
	}
}

func setupRangeTest(t *testing.T, bs Blobstore, data []byte) {
	_, err := PutBytes(bs, key, data)

	if err != nil {
		t.FailNow()
	}
}

func testGetRange(t *testing.T, bs Blobstore, br BlobRange, expected []byte) {
	retrieved, _, err := GetBytes(bs, key, br)

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
		setupRangeTest(t, bsTest.bs, testData)
	}

	// test full range
	for _, bsTest := range tests {
		testGetRange(t, bsTest.bs, AllRange, rangeData(0, maxValue))
	}

	// test first 2048 bytes (1024 shorts)
	for _, bsTest := range tests {
		testGetRange(t, bsTest.bs, NewBlobRange(0, 2048), rangeData(0, 1024))
	}

	// test range of values from 1024 to 2048 stored in bytes 2048 to 4096 of the original testData
	for _, bsTest := range tests {
		testGetRange(t, bsTest.bs, NewBlobRange(2*1024, 2*1024), rangeData(1024, 2048))
	}

	// test the last 2048 bytes of data which will be the last 1024 shorts
	for _, bsTest := range tests {
		testGetRange(t, bsTest.bs, NewBlobRange(-2*1024, 0), rangeData(maxValue-1024, maxValue))
	}

	// test the range beginning 2048 bytes from the end of size 512 which will be shorts 1024 from the end til 768 from the end
	for _, bsTest := range tests {
		testGetRange(t, bsTest.bs, NewBlobRange(-2*1024, 512), rangeData(maxValue-1024, maxValue-768))
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
