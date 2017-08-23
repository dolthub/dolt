package cbor

import "bytes"
import "encoding/base64"
import "encoding/hex"
import "encoding/json"
import "fmt"
import "log"
import "math"
import "math/big"
import "os"
import "reflect"
import "strings"
import "testing"

type testVector struct {
	Cbor string
	Hex string
	Roundtrip bool
	Decoded interface{}
	Diagnostic string
}

var errpath string = "../test-vectors/appendix_a.json"

func readVectors(t *testing.T) ([]testVector, error) {
	fin, err := os.Open(errpath)
	if err != nil {
		t.Error("could not open test vectors at: ", errpath)
		return nil, err
	}
	jd := json.NewDecoder(fin)
	jd.UseNumber()
	they := new([]testVector)
	err = jd.Decode(they)
	return *they, err
}


func jeq(jsonv, cborv interface{}, t *testing.T) bool {
	switch i := cborv.(type) {
	case uint64:
		switch jv := jsonv.(type) {
		case int:
			return (jv >= 0) && (uint64(jv) == i)
		case uint64:
			return i == jv
		case float64:
			return math.Abs(float64(i) - jv) < math.Max(math.Abs(jv / 1000000000.0), 1.0/1000000000.0)
		case json.Number:
			return jv.String() == fmt.Sprintf("%d", i)
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case big.Int:
		switch jv := jsonv.(type) {
		case json.Number:
			return jv.String() == i.String()
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case int64:
		switch jv := jsonv.(type) {
		case int:
			return int64(jv) == i
		case json.Number:
			return jv.String() == fmt.Sprintf("%d", i)
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case float32:
		switch jv := jsonv.(type) {
		case json.Number:
			//return jv.String() == fmt.Sprintf("%f", i)
			fv, err := jv.Float64()
			if err != nil {
				t.Errorf("error getting json float: %s", err)
				return false;
			}
			return fv == float64(i)
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case float64:
		switch jv := jsonv.(type) {
		case json.Number:
			//return jv.String() == fmt.Sprintf("%f", i)
			fv, err := jv.Float64()
			if err != nil {
				t.Errorf("error getting json float: %s", err)
				return false;
			}
			return fv == i
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case bool:
		switch jv := jsonv.(type) {
		case bool:
			return jv == i
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case string:
		switch jv := jsonv.(type) {
		case string:
			return jv == i
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case []interface{}:
		switch jv := jsonv.(type) {
		case []interface{}:
			if len(i) != len(jv) {
				return false
			}
			for cai, cav := range(i) {
				if !jeq(jv[cai], cav, t) {
					t.Errorf("array mismatch at [%d]: json=%#v cbor=%#v", cai, jv[cai], cav)
					return false
				}
/*
				if fmt.Sprintf("%v", cav) != fmt.Sprintf("%v", jv[cai]) {
					return false
				}
*/
			}
			return true
		default:
			if reflect.DeepEqual(cborv, jsonv) {
				return true
			}
			t.Errorf("wat types cbor %T json %T", cborv, jsonv);
			return false
		}
	case nil:
		switch jv := jsonv.(type) {
		case nil:
			return true;
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jv);
			return false
		}
	case map[interface{}]interface{}:
		switch jv := jsonv.(type) {
		case map[string]interface{}:
			for jmk, jmv := range(jv) {
				cmv, ok := i[jmk]
				if !ok {
					t.Errorf("json key %v missing from cbor", jmk)
					return false
				}
				if !jeq(jmv, cmv, t) {
					t.Errorf("map key=%#v values: json=%#v cbor=%#v", jmk, jmv, cmv)
					return false
				}
/*
				if !reflect.DeepEqual(cmv, jmv) {
					t.Errorf("map key=%#v values: json=%#v cbor=%#v", jmk, jmv, cmv)
					return false
				}
*/
			}
			return true
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jv);
			return false
		}
	case []byte:
		switch jv := jsonv.(type) {
		case []byte:
			return bytes.Equal(i, jv)
		default:
			t.Errorf("wat types cbor %T json %T", cborv, jv);
			return false
		}
	default:
		eq := reflect.DeepEqual(jsonv, cborv)
		if ! eq {
			log.Printf("unexpected cbor type %T = %#v", cborv, cborv)
			t.Errorf("unexpected cbor type %T = %#v", cborv, cborv)
		}
		return eq
		//return fmt.Sprintf("%v", jsonv) == fmt.Sprintf("%v", cborv)
		//return jsonv == cborv
	}
}


func TestDecodeVectors(t *testing.T) {
	//t.Parallel()

	t.Log("test standard decode vectors")
	they, err := readVectors(t)
	if err != nil {
		t.Fatal("could not load test vectors:", err)
		return
	}
	t.Logf("got %d test vectors", len(they))
	if len(they) <= 0 {
		t.Fatal("got no test vectors")
		return
	}
	for i, testv := range(they) {
		if testv.Decoded != nil && len(testv.Cbor) > 0 {
			//log.Printf("hex %s", testv.Hex)
			t.Logf("hex %s", testv.Hex)
			bin, err := base64.StdEncoding.DecodeString(testv.Cbor)
			if err != nil {
				t.Logf("test[%d] %#v", i, testv)
				t.Logf("decoding [%d] %#v ...\n", i, testv.Cbor)
				t.Fatal("could not decode test vector b64")
				return
			}
			ring := NewDecoder(bytes.NewReader(bin))
			var cborObject interface{}
			err = ring.Decode(&cborObject)
			if err != nil {
				t.Logf("test[%d] %#v", i, testv)
				t.Logf("decoding [%d] %#v ...\n", i, testv.Cbor)
				t.Fatalf("error decoding cbor: %v", err)
				return
			}
			if !jeq(testv.Decoded, cborObject, t) {
				//t.Logf("test[%d] %#v", i, testv)
				t.Logf("decoding [%d] %#v ...\n", i, testv.Cbor)
				t.Errorf("json %T %#v != cbor %T %#v", testv.Decoded, testv.Decoded, cborObject, cborObject)
				t.Logf("------")
			}
		}
	}
}


type RefTestOb struct {
	AString string
	BInt int
	CUint uint64
	DFloat float64
	EIntArray []int
	FStrIntMap map[string]int
	GBool bool
}

type privateTestOb struct {
	privateInt int
	PubInt int
	PubSkip int `cbor:"-"`
}


func checkObOne(ob RefTestOb, t *testing.T) bool {
	ok := true
	if ob.AString != "astring val" {
		t.Errorf("AString wanted \"astring val\" but got %#v", ob.AString)
		ok = false
	}
	if ob.BInt != -33 {
		t.Errorf("BInt wanted -33 but got %#v", ob.BInt)
		ok = false
	}
	if ob.CUint != 42 {
		t.Errorf("CUint wanted 42 but got %#v", ob.CUint)
		ok = false
	}
	if ob.DFloat != 0.25 {
		t.Errorf("DFloat wanted 02.5 but got %#v", ob.DFloat)
		ok = false
	}
	return ok
}


const (
	reflexObOneJson = "{\"astring\": \"astring val\", \"bint\": -33, \"cuint\": 42, \"dfloat\": 0.25, \"eintarray\": [1,2,3], \"fstrintmap\":{\"a\":13, \"b\":14}, \"gbool\": false}"
	reflexObOneCborB64 = "p2dhc3RyaW5na2FzdHJpbmcgdmFsamZzdHJpbnRtYXCiYWENYWIOZWdib29s9GRiaW50OCBpZWludGFycmF5gwECA2VjdWludBgqZmRmbG9hdPs/0AAAAAAAAA=="
)

var referenceObOne RefTestOb = RefTestOb{
	"astring val", -33, 42, 0.25, []int{1,2,3},
	map[string]int{"a":13, "b": 14}, false}

/*
#python
import json
import cbor
import base64
# copy in the above json string literal here:
jsonstr = 
print base64.b64encode(cbor.dumps(json.loads(jsonstr)))
*/

func TestDecodeReflectivelyOne(t *testing.T) {
	//t.Parallel()
	t.Log("test decode reflectively one")

	var err error
	
	jd := json.NewDecoder(strings.NewReader(reflexObOneJson))
	jd.UseNumber()
	they := RefTestOb{}
	err = jd.Decode(&they)
	if err != nil {
		t.Fatal("could not decode json", err)
		return
	}

	t.Log("check json")
	if !checkObOne(they, t) {
		return
	}

	bin, err := base64.StdEncoding.DecodeString(reflexObOneCborB64)
	if err != nil {
		t.Fatal("error decoding cbor b64", err)
		return
	}
	ring := NewDecoder(bytes.NewReader(bin))
	cob := RefTestOb{}
	err = ring.Decode(&cob)
	if err != nil {
		t.Fatal("error decoding cbor", err)
		return
	}
	t.Log("check cbor")
	if !checkObOne(they, t) {
		return
	}
}

func TestEncodeWithPrivate(t *testing.T) {
	//t.Parallel()
	t.Log("test encode with private")

	var err error
	ob := privateTestOb{1,2,3}

	writeTarget := &bytes.Buffer{}
	writeTarget.Grow(20000)
	err = Encode(writeTarget, ob)
	if err != nil {
		t.Errorf("failed on encode: %s", err)
		return
	}

	{
		destmap := make(map[string]interface{})
		scratch := writeTarget.Bytes()
		dec := NewDecoder(bytes.NewReader(scratch))
		err = dec.Decode(&destmap)
		if err != nil {
			t.Errorf("failed on decode: %s", err)
			return
		}
		pi, ok := destmap["privateInt"]
		if ok {
			t.Errorf("destmap shouldn't have privateInt %v: %#v", pi, destmap)
		}
		pubskip, ok := destmap["PubSkip"]
		if ok {
			t.Errorf("destmap shouldn't have PubSkip %v: %#v", pubskip, destmap)
		}
	}

	xo := privateTestOb{-1,-1,-1}
	scratch := writeTarget.Bytes()
	dec := NewDecoder(bytes.NewReader(scratch))
	err = dec.Decode(&xo)
	if err != nil {
		t.Errorf("failed on decode: %s", err)
		return
	}
	if xo.privateInt != -1 {
		t.Errorf("privateInt is %d, wanted -1", xo.privateInt)
	}
	if xo.PubSkip != -1 {
		t.Errorf("PubSkip is %d, wanted -1", xo.PubSkip)
	}
}

func objectSerializedObject(t *testing.T, ob interface{}) {
	out := reflect.Indirect(reflect.New(reflect.TypeOf(ob))).Interface()
	//t.Logf("oso ob T %T %#v, out T %T %#v", ob, ob, out, out)
	objectSerializedTargetObject(t, ob, &out)
	if !jeq(ob, out, t) {
		//
		t.Errorf("%#v != %#v", ob, out)
	}
}
func objectSerializedTargetObject(t *testing.T, ob interface{}, out interface{}) {
	t.Logf("oso ob T %T %#v", ob, ob)
	t.Logf("   out T %T %#v", out, out)
	//scratch := make([]byte, 0)
	//writeTarget := bytes.NewBuffer(scratch)
	writeTarget := &bytes.Buffer{}
	writeTarget.Grow(20000)
	err := Encode(writeTarget, ob)
	if err != nil {
		t.Errorf("failed on encode: %s", err)
		return
	}

	scratch := writeTarget.Bytes()
	dec := NewDecoder(bytes.NewReader(scratch))
	err = dec.Decode(out)

	t.Logf("oso ob T %T %#v", ob, ob)
	t.Logf("   out T %T %#v", out, out)

	t.Log(hex.EncodeToString(scratch))
	if err != nil {
		t.Errorf("failed on decode: %s", err)
		return
	}
}

func TestOSO(t *testing.T) {
	t.Log("test OSO")
	objectSerializedObject(t, 0)
	objectSerializedObject(t, 1)
	objectSerializedObject(t, 2)
	objectSerializedObject(t, -1)
	objectSerializedObject(t, true)
	objectSerializedObject(t, false)
	// TODO: some of these don't quite work yet
	//objectSerializedObject(t, nil)
	objectSerializedObject(t, []interface{}{})
	//objectSerializedObject(t, []int{})
	//objectSerializedObject(t, []int{1,2,3})
	objectSerializedObject(t, "hello")
	objectSerializedObject(t, []byte{1,3,2})
	//objectSerializedObject(t, RefTestOb{"hi", -1000, 137, 0.5, nil, nil, true})
//	objectSerializedObject(t, )
}


func TestRefStruct(t *testing.T) {
	t.Log("test hard")
	trto := RefTestOb{}
	objectSerializedTargetObject(t, referenceObOne, &trto)
	checkObOne(trto, t)
}


func TestArrays(t *testing.T) {
	t.Log("test arrays")
	
	// okay, sooo, slices
	ia := []int{1,2,3}
	tia := []int{}
	objectSerializedTargetObject(t, ia, &tia)
	if ! reflect.DeepEqual(ia, tia) {
		t.Errorf("int array %#v != %#v", ia, tia)
	}

	// actual arrays
	xa := [3]int{4,5,6}
	txa := [3]int{}
	objectSerializedTargetObject(t, xa, &txa)
	if ! reflect.DeepEqual(xa, txa) {
		t.Errorf("int array %#v != %#v", xa, txa)
	}
	
	oa := [3]interface{}{"hi", 2, -3.14}
	toa := [3]interface{}{}
	objectSerializedTargetObject(t, oa, &toa)
	if toa[0] != "hi" {
		t.Errorf("[3]interface{} [0] wanted \"hi\" got %#v", toa[0])
	}
	if toa[1] != uint64(2) {
		t.Errorf("[3]interface{} [0] wanted 2 got %#v", toa[1])
	}
	if toa[2] != -3.14 {
		t.Errorf("[3]interface{} [0] wanted -3.14 got %#v", toa[2])
	}
}

type TaggedStruct struct {
	One string `json:"m_one_s"`
	Two int `json:"bunnies,omitempty"`
	Three float64 `json:"htree_json" cbor:"three_cbor"`
}

func PracticalInt64(xi interface{}) (int64, bool) {
	switch i := xi.(type) {
	//case int, int8, int16, int32, int64, uint8, uint16, uint32:
		//oi, ok := i.(int64)
		//return oi, ok
	case int:
		return int64(i), true
	case int64:
		return int64(i), true
	case uint64:
		if i < 0x7fffffffffffffff {
			return int64(i), true
		}
		return 0, false
	}
	return 0, false
}

func ms(d map[string]interface{}, k string, t *testing.T) (string, bool) {
	if d == nil {
		t.Error("nil map")
		return "", false
	}
	ob, ok := d[k]
	if !ok {
		t.Errorf("map missing key %v", k)
		return "", false
	}
	xs, ok := ob.(string)
	return xs, ok
}
// I wish go had templates!
func mi(d map[string]interface{}, k string, t *testing.T) (int64, bool) {
	if d == nil {
		t.Error("nil map")
		return 0, false
	}
	ob, ok := d[k]
	if !ok {
		t.Errorf("map missing key %v", k)
		return 0, false
	}
	//xs, ok := ob.(int)
	xs, ok := PracticalInt64(ob)
	return xs, ok
}
func mf64(d map[string]interface{}, k string, t *testing.T) (float64, bool) {
	if d == nil {
		t.Error("nil map")
		return 0, false
	}
	ob, ok := d[k]
	if !ok {
		t.Errorf("map missing key %v", k)
		return 0, false
	}
	xs, ok := ob.(float64)
	return xs, ok
}

func TestStructTags(t *testing.T) {
	t.Log("StructTags")
	
	ob := TaggedStruct{"hello", 42, 6.28}
	
	blob, err := Dumps(ob)
	if err != nil {
		t.Error(err)
	}

	mapo := make(map[string]interface{})
	err = Loads(blob, &mapo)
	if err != nil {
		t.Error(err)
	}
	xs, ok := ms(mapo, "m_one_s", t)
	if (!ok) || (xs != "hello") {
		t.Errorf("failed to get m_one_s from %#v", mapo)
	}
	xi, ok := mi(mapo, "bunnies", t)
	if (!ok) || (xi != 42) {
		t.Errorf("failed to get bunnies from %#v", mapo)
	}
	xf, ok := mf64(mapo, "three_cbor", t)
	if (!ok) || (xf != 6.28) {
		t.Errorf("failed to get three_cbor from %#v", mapo)
	}

	ob2 := TaggedStruct{}
	err = Loads(blob, &ob2)
	if err != nil {
		t.Error(err)
	}
	if ob != ob2 {
		t.Errorf("a!=b %#v != %#v", ob, ob2)
	}
}
