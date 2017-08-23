// Should be roughly like encoding/gob
// encoding/json has an inferior interface that only works on whole messages to/from whole blobs at once. Reader/Writer based interfaces are better.

package cbor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"reflect"
	"sort"
	"strings"
)

var typeMask byte = 0xE0
var infoBits byte = 0x1F

const (
	MajorTypeUint   byte = 0
	MajorTypeNegInt byte = iota
	MajorTypeBytes
	MajorTypeText
	MajorTypeArray
	MajorTypeMap
	MajorTypeTag
	MajorTypeSimple
	MajorTypeFloat byte = MajorTypeSimple
)

const (
	SimpleValueFalse byte = 20
	SimpleValueTrue  byte = iota
	SimpleValueNull
	SimpleValueUndefined
)

const (
	OpcodeBreak byte = 0x1F
)

/* type values */
var cborUint byte = 0x00
var cborNegint byte = 0x20
var cborBytes byte = 0x40
var cborText byte = 0x60
var cborArray byte = 0x80
var cborMap byte = 0xA0
var cborTag byte = 0xC0
var cbor7 byte = 0xE0

/* cbor7 values */
const (
	cborFalse byte = 20
	cborTrue  byte = 21
	cborNull  byte = 22
)

/* info bits */
var int8Follows byte = 24
var int16Follows byte = 25
var int32Follows byte = 26
var int64Follows byte = 27
var varFollows byte = 31

/* tag values */
var tagBignum uint64 = 2
var tagNegBignum uint64 = 3
var tagDecimal uint64 = 4
var tagBigfloat uint64 = 5

// TODO: honor encoding.BinaryMarshaler interface and encapsulate blob returned from that.

// Load one object into v
func Loads(blob []byte, v interface{}) error {
	dec := NewDecoder(bytes.NewReader(blob))
	return dec.Decode(v)
}

type TagDecoder interface {
	// Handle things which match this.
	//
	// Setup like this:
	// var dec Decoder
	// var myTagDec TagDecoder
	// dec.TagDecoders[myTagDec.GetTag()] = myTagDec
	GetTag() uint64

	// Sub-object will be decoded onto the returned object.
	DecodeTarget() interface{}

	// Run after decode onto DecodeTarget has happened.
	// The return value from this is returned in place of the
	// raw decoded object.
	PostDecode(interface{}) (interface{}, error)
}

type Decoder struct {
	rin io.Reader

	// tag byte
	c []byte

	// many values fit within the next 8 bytes
	b8 []byte

	// Extra processing for CBOR TAG objects.
	TagDecoders map[uint64]TagDecoder
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		rin:         r,
		c:           make([]byte, 1),
		b8:          make([]byte, 8),
		TagDecoders: make(map[uint64]TagDecoder),
	}
}
func (dec *Decoder) Decode(v interface{}) error {
	rv := reflect.ValueOf(v)

	return dec.DecodeAny(newReflectValue(rv))
}

type DecodeValue interface {
	// Before decoding, check if there is no error
	Prepare() error

	// Got binary string
	SetBytes(buf []byte) error

	// Got a number (different formats)
	SetBignum(x *big.Int) error
	SetUint(u uint64) error
	SetInt(i int64) error
	SetFloat32(f float32) error
	SetFloat64(d float64) error

	// Got null
	SetNil() error

	// Got boolean
	SetBool(b bool) error

	// Got text string
	SetString(s string) error

	// Got a Map (beginning)
	CreateMap() (DecodeValueMap, error)

	// Got an array (beginning)
	CreateArray(makeLength int) (DecodeValueArray, error)

	// Got a tag
	CreateTag(aux uint64, decoder TagDecoder) (DecodeValue, interface{}, error)

	// Got the tag value (maybe transformed by TagDecoder.PostDecode)
	SetTag(aux uint64, v DecodeValue, decoder TagDecoder, i interface{}) error
}

type DecodeValueMap interface {
	// Got a map key
	CreateMapKey() (DecodeValue, error)

	// Got a map value
	CreateMapValue(key DecodeValue) (DecodeValue, error)

	// Got a key / value pair
	SetMap(key, val DecodeValue) error

	// The map is at the end
	EndMap() error
}

type DecodeValueArray interface {
	// Got an array item
	GetArrayValue(index uint64) (DecodeValue, error)

	// After the array item is decoded
	AppendArray(value DecodeValue) error

	// The array is at the end
	EndArray() error
}

type reflectValue struct {
	v reflect.Value
}

type MemoryValue struct {
	reflectValue
	Value interface{}
}

func NewMemoryValue(value interface{}) *MemoryValue {
	res := &MemoryValue{
		reflectValue{reflect.ValueOf(nil)},
		value,
	}
	res.v = reflect.ValueOf(&res.Value)
	return res
}

func (mv *MemoryValue) ReflectValue() reflect.Value {
	return mv.v
}

func newReflectValue(rv reflect.Value) *reflectValue {
	return &reflectValue{rv}
}

func (r *reflectValue) Prepare() error {
	rv := r.v
	if (!rv.CanSet()) && (rv.Kind() != reflect.Ptr || rv.IsNil()) {
		return &InvalidUnmarshalError{rv.Type()}
	}
	return nil
}

func (dec *Decoder) DecodeAny(v DecodeValue) error {
	var err error

	_, err = io.ReadFull(dec.rin, dec.c)
	if err != nil {
		return err
	}

	if err := v.Prepare(); err != nil {
		return err
	}

	return dec.innerDecodeC(v, dec.c[0])
}

func (dec *Decoder) handleInfoBits(cborInfo byte) (uint64, error) {
	var aux uint64

	if cborInfo <= 23 {
		aux = uint64(cborInfo)
		return aux, nil
	} else if cborInfo == int8Follows {
		didread, err := io.ReadFull(dec.rin, dec.b8[:1])
		if didread == 1 {
			aux = uint64(dec.b8[0])
		}
		return aux, err
	} else if cborInfo == int16Follows {
		didread, err := io.ReadFull(dec.rin, dec.b8[:2])
		if didread == 2 {
			aux = (uint64(dec.b8[0]) << 8) | uint64(dec.b8[1])
		}
		return aux, err
	} else if cborInfo == int32Follows {
		didread, err := io.ReadFull(dec.rin, dec.b8[:4])
		if didread == 4 {
			aux = (uint64(dec.b8[0]) << 24) |
				(uint64(dec.b8[1]) << 16) |
				(uint64(dec.b8[2]) << 8) |
				uint64(dec.b8[3])
		}
		return aux, err
	} else if cborInfo == int64Follows {
		didread, err := io.ReadFull(dec.rin, dec.b8)
		if didread == 8 {
			var shift uint = 56
			i := 0
			aux = uint64(dec.b8[i]) << shift
			for i < 7 {
				i += 1
				shift -= 8
				aux |= uint64(dec.b8[i]) << shift
			}
		}
		return aux, err
	}
	return 0, nil
}

func (dec *Decoder) innerDecodeC(rv DecodeValue, c byte) error {
	cborType := c & typeMask
	cborInfo := c & infoBits

	aux, err := dec.handleInfoBits(cborInfo)
	if err != nil {
		log.Printf("error in handleInfoBits: %v", err)
		return err
	}
	//log.Printf("cborType %x cborInfo %d aux %x", cborType, cborInfo, aux)

	if cborType == cborUint {
		return rv.SetUint(aux)
	} else if cborType == cborNegint {
		if aux > 0x7fffffffffffffff {
			//return errors.New(fmt.Sprintf("cannot represent -%d", aux))
			bigU := &big.Int{}
			bigU.SetUint64(aux)
			minusOne := big.NewInt(-1)
			bn := &big.Int{}
			bn.Sub(minusOne, bigU)
			//log.Printf("built big negint: %v", bn)
			return rv.SetBignum(bn)
		}
		return rv.SetInt(-1 - int64(aux))
	} else if cborType == cborBytes {
		//log.Printf("cborType %x bytes cborInfo %d aux %x", cborType, cborInfo, aux)
		if cborInfo == varFollows {
			parts := make([][]byte, 0, 1)
			allsize := 0
			subc := []byte{0}
			for true {
				_, err = io.ReadFull(dec.rin, subc)
				if err != nil {
					log.Printf("error reading next byte for bar bytes")
					return err
				}
				if subc[0] == 0xff {
					// done
					var out []byte = nil
					if len(parts) == 0 {
						out = make([]byte, 0)
					} else {
						pos := 0
						out = make([]byte, allsize)
						for _, p := range parts {
							pos += copy(out[pos:], p)
						}
					}
					return rv.SetBytes(out)
				} else {
					var subb []byte = nil
					if (subc[0] & typeMask) != cborBytes {
						return fmt.Errorf("sub of var bytes is type %x, wanted %x", subc[0], cborBytes)
					}
					err = dec.innerDecodeC(newReflectValue(reflect.ValueOf(&subb)), subc[0])
					if err != nil {
						log.Printf("error decoding sub bytes")
						return err
					}
					allsize += len(subb)
					parts = append(parts, subb)
				}
			}
		} else {
			val := make([]byte, aux)
			_, err = io.ReadFull(dec.rin, val)
			if err != nil {
				return err
			}
			// Don't really care about count, ReadFull will make it all or none and we can just fall out with whatever error
			return rv.SetBytes(val)
			/*if (rv.Kind() == reflect.Slice) && (rv.Type().Elem().Kind() == reflect.Uint8) {
				rv.SetBytes(val)
			} else {
				return fmt.Errorf("cannot write []byte to k=%s %s", rv.Kind().String(), rv.Type().String())
			}*/
		}
	} else if cborType == cborText {
		return dec.decodeText(rv, cborInfo, aux)
	} else if cborType == cborArray {
		return dec.decodeArray(rv, cborInfo, aux)
	} else if cborType == cborMap {
		return dec.decodeMap(rv, cborInfo, aux)
	} else if cborType == cborTag {
		/*var innerOb interface{}*/
		ic := []byte{0}
		_, err = io.ReadFull(dec.rin, ic)
		if err != nil {
			return err
		}
		if aux == tagBignum {
			bn, err := dec.decodeBignum(ic[0])
			if err != nil {
				return err
			}
			return rv.SetBignum(bn)
		} else if aux == tagNegBignum {
			bn, err := dec.decodeBignum(ic[0])
			if err != nil {
				return err
			}
			minusOne := big.NewInt(-1)
			bnOut := &big.Int{}
			bnOut.Sub(minusOne, bn)
			return rv.SetBignum(bnOut)
		} else if aux == tagDecimal {
			log.Printf("TODO: directly read bytes into decimal")
		} else if aux == tagBigfloat {
			log.Printf("TODO: directly read bytes into bigfloat")
		} else {
			decoder := dec.TagDecoders[aux]
			var target interface{}
			var trv DecodeValue
			var err error

			trv, target, err = rv.CreateTag(aux, decoder)
			if err != nil {
				return err
			}

			err = dec.innerDecodeC(trv, ic[0])
			if err != nil {
				return err
			}

			return rv.SetTag(aux, trv, decoder, target)
		}
		return nil
	} else if cborType == cbor7 {
		if cborInfo == int16Follows {
			exp := (aux >> 10) & 0x01f
			mant := aux & 0x03ff
			var val float64
			if exp == 0 {
				val = math.Ldexp(float64(mant), -24)
			} else if exp != 31 {
				val = math.Ldexp(float64(mant+1024), int(exp-25))
			} else if mant == 0 {
				val = math.Inf(1)
			} else {
				val = math.NaN()
			}
			if (aux & 0x08000) != 0 {
				val = -val
			}
			return rv.SetFloat64(val)
		} else if cborInfo == int32Follows {
			f := math.Float32frombits(uint32(aux))
			return rv.SetFloat32(f)
		} else if cborInfo == int64Follows {
			d := math.Float64frombits(aux)
			return rv.SetFloat64(d)
		} else if cborInfo == cborFalse {
			return rv.SetBool(false)
		} else if cborInfo == cborTrue {
			return rv.SetBool(true)
		} else if cborInfo == cborNull {
			return rv.SetNil()
		}
	}

	return err
}

func (dec *Decoder) decodeText(rv DecodeValue, cborInfo byte, aux uint64) error {
	var err error
	if cborInfo == varFollows {
		parts := make([]string, 0, 1)
		subc := []byte{0}
		for true {
			_, err = io.ReadFull(dec.rin, subc)
			if err != nil {
				log.Printf("error reading next byte for var text")
				return err
			}
			if subc[0] == 0xff {
				// done
				joined := strings.Join(parts, "")
				return rv.SetString(joined)
			} else {
				var subtext interface{}
				err = dec.innerDecodeC(newReflectValue(reflect.ValueOf(&subtext)), subc[0])
				if err != nil {
					log.Printf("error decoding subtext")
					return err
				}
				st, ok := subtext.(string)
				if ok {
					parts = append(parts, st)
				} else {
					return fmt.Errorf("var text sub element not text but %T", subtext)
				}
			}
		}
	} else {
		raw := make([]byte, aux)
		_, err = io.ReadFull(dec.rin, raw)
		xs := string(raw)
		return rv.SetString(xs)
	}
	return errors.New("internal error in decodeText, shouldn't get here")
}

type mapAssignable interface {
	ReflectValueForKey(key interface{}) (*reflect.Value, bool)
	SetReflectValueForKey(key interface{}, value reflect.Value) error
}

type mapReflectValue struct {
	reflect.Value
}

func (irv *mapReflectValue) ReflectValueForKey(key interface{}) (*reflect.Value, bool) {
	//var x interface{}
	//rv := reflect.ValueOf(&x)
	rv := reflect.New(irv.Type().Elem())
	return &rv, true
}
func (irv *mapReflectValue) SetReflectValueForKey(key interface{}, value reflect.Value) error {
	//log.Printf("k T %T v%#v, v T %s v %#v", key, key, value.Type().String(), value.Interface())
	krv := reflect.Indirect(reflect.ValueOf(key))
	vrv := reflect.Indirect(value)
	//log.Printf("irv T %s v %#v", irv.Type().String(), irv.Interface())
	//log.Printf("k T %s v %#v, v T %s v %#v", krv.Type().String(), krv.Interface(), vrv.Type().String(), vrv.Interface())
	if krv.Kind() == reflect.Interface {
		krv = krv.Elem()
		//log.Printf("ke T %s v %#v", krv.Type().String(), krv.Interface())
	}
	if (krv.Kind() == reflect.Slice) || (krv.Kind() == reflect.Array) {
		//log.Printf("key is slice or array")
		if krv.Type().Elem().Kind() == reflect.Uint8 {
			//log.Printf("key is []uint8")
			ks := string(krv.Bytes())
			krv = reflect.ValueOf(ks)
		}
	}
	irv.SetMapIndex(krv, vrv)

	return nil
}

type structAssigner struct {
	Srv reflect.Value

	//keyType reflect.Type
}

func (sa *structAssigner) ReflectValueForKey(key interface{}) (*reflect.Value, bool) {
	var skey string
	switch tkey := key.(type) {
	case string:
		skey = tkey
	case *string:
		skey = *tkey
	default:
		log.Printf("rvfk key is not string, got %T", key)
		return nil, false
	}

	ft := sa.Srv.Type()
	numFields := ft.NumField()
	for i := 0; i < numFields; i++ {
		sf := ft.Field(i)
		fieldname, ok := fieldname(sf)
		if !ok {
			continue
		}
		if (fieldname == skey) || strings.EqualFold(fieldname, skey) {
			fieldVal := sa.Srv.FieldByName(sf.Name)
			if !fieldVal.CanSet() {
				log.Printf("cannot set field %s for key %s", sf.Name, skey)
				return nil, false
			}
			return &fieldVal, true
		}
	}
	return nil, false
}
func (sa *structAssigner) SetReflectValueForKey(key interface{}, value reflect.Value) error {
	return nil
}

func (dec *Decoder) setMapKV(dvm DecodeValueMap, krv DecodeValue) error {
	var err error
	val, err := dvm.CreateMapValue(krv)
	if err != nil {
		var throwaway interface{}
		err = dec.Decode(&throwaway)
		if err != nil {
			return err
		}
		return nil
	}
	err = dec.DecodeAny(val)
	if err != nil {
		log.Printf("error decoding map val: T %T v %#v", val, val)
		return err
	}
	err = dvm.SetMap(krv, val)
	if err != nil {
		log.Printf("error setting value")
		return err
	}

	return nil
}

func (r *reflectValue) CreateMap() (DecodeValueMap, error) {
	rv := r.v
	var drv reflect.Value
	if rv.Kind() == reflect.Ptr {
		drv = reflect.Indirect(rv)
	} else {
		drv = rv
	}
	//log.Print("decode map into d ", drv.Type().String())

	// inner reflect value
	var irv reflect.Value
	var ma mapAssignable

	var keyType reflect.Type

	switch drv.Kind() {
	case reflect.Interface:
		//log.Print("decode map into interface ", drv.Type().String())
		// TODO: maybe I should make this map[string]interface{}
		nob := make(map[interface{}]interface{})
		irv = reflect.ValueOf(nob)
		ma = &mapReflectValue{irv}
		keyType = irv.Type().Key()
	case reflect.Struct:
		//log.Print("decode map into struct ", drv.Type().String())
		ma = &structAssigner{drv}
		keyType = reflect.TypeOf("")
	case reflect.Map:
		//log.Print("decode map into map ", drv.Type().String())
		if drv.IsNil() {
			if drv.CanSet() {
				drv.Set(reflect.MakeMap(drv.Type()))
			} else {
				return nil, fmt.Errorf("target map is nil and not settable")
			}
		}
		keyType = drv.Type().Key()
		ma = &mapReflectValue{drv}
	default:
		return nil, fmt.Errorf("can't read map into %s", rv.Type().String())
	}

	return &reflectValueMap{drv, irv, ma, keyType}, nil
}

type reflectValueMap struct {
	drv     reflect.Value
	irv     reflect.Value
	ma      mapAssignable
	keyType reflect.Type
}

func (r *reflectValueMap) CreateMapKey() (DecodeValue, error) {
	return newReflectValue(reflect.New(r.keyType)), nil
}

func (r *reflectValueMap) CreateMapValue(key DecodeValue) (DecodeValue, error) {
	var err error
	v, ok := r.ma.ReflectValueForKey(key.(*reflectValue).v.Interface())
	if !ok {
		err = fmt.Errorf("Could not reflect value for key")
	}
	return newReflectValue(*v), err
}

func (r *reflectValueMap) SetMap(key, val DecodeValue) error {
	return r.ma.SetReflectValueForKey(key.(*reflectValue).v.Interface(), val.(*reflectValue).v)
}

func (r *reflectValueMap) EndMap() error {
	if r.drv.Kind() == reflect.Interface {
		r.drv.Set(r.irv)
	}
	return nil
}

func (dec *Decoder) decodeMap(rv DecodeValue, cborInfo byte, aux uint64) error {
	//log.Print("decode map into   ", rv.Type().String())
	// dereferenced reflect value
	var dvm DecodeValueMap
	var err error

	dvm, err = rv.CreateMap()
	if err != nil {
		return err
	}

	if cborInfo == varFollows {
		subc := []byte{0}
		for true {
			_, err = io.ReadFull(dec.rin, subc)
			if err != nil {
				log.Printf("error reading next byte for var text")
				return err
			}
			if subc[0] == 0xff {
				// Done
				break
			} else {
				//var key interface{}
				krv, err := dvm.CreateMapKey()
				if err != nil {
					return err
				}
				//var val interface{}
				err = dec.innerDecodeC(krv, subc[0])
				if err != nil {
					log.Printf("error decoding map key V, %s", err)
					return err
				}

				err = dec.setMapKV(dvm, krv)
				if err != nil {
					return err
				}
			}
		}
	} else {
		var i uint64
		for i = 0; i < aux; i++ {
			//var key interface{}
			krv, err := dvm.CreateMapKey()
			if err != nil {
				return err
			}
			//var val interface{}
			//err = dec.Decode(&key)
			err = dec.DecodeAny(krv)
			if err != nil {
				log.Printf("error decoding map key #, %s", err)
				return err
			}
			err = dec.setMapKV(dvm, krv)
			if err != nil {
				return err
			}
		}
	}

	return dvm.EndMap()
}

func (r *reflectValue) CreateArray(makeLength int) (DecodeValueArray, error) {
	var rv reflect.Value = r.v

	if rv.Kind() == reflect.Ptr {
		rv = reflect.Indirect(rv)
	}

	// inner reflect value
	var irv reflect.Value
	var elemType reflect.Type

	switch rv.Kind() {
	case reflect.Interface:
		// make a slice
		nob := make([]interface{}, 0, makeLength)
		irv = reflect.ValueOf(nob)
		elemType = irv.Type().Elem()
	case reflect.Slice:
		// we have a slice
		irv = rv
		elemType = irv.Type().Elem()
	case reflect.Array:
		// no irv, no elemType
	default:
		return nil, fmt.Errorf("can't read array into %s", rv.Type().String())
	}

	return &reflectValueArray{rv, makeLength, irv, elemType, 0}, nil
}

type reflectValueArray struct {
	rv         reflect.Value
	makeLength int
	irv        reflect.Value
	elemType   reflect.Type
	arrayPos   int
}

func (r *reflectValueArray) GetArrayValue(index uint64) (DecodeValue, error) {
	if r.rv.Kind() == reflect.Array {
		return &reflectValue{r.rv.Index(r.arrayPos)}, nil
	} else {
		return &reflectValue{reflect.New(r.elemType)}, nil
	}
}

func (r *reflectValueArray) AppendArray(subrv DecodeValue) error {
	if r.rv.Kind() == reflect.Array {
		r.arrayPos++
	} else {
		r.irv = reflect.Append(r.irv, reflect.Indirect(subrv.(*reflectValue).v))
	}
	return nil
}

func (r *reflectValueArray) EndArray() error {
	if r.rv.Kind() != reflect.Array {
		r.rv.Set(r.irv)
	}
	return nil
}

func (dec *Decoder) decodeArray(rv DecodeValue, cborInfo byte, aux uint64) error {

	var err error
	var dva DecodeValueArray

	var makeLength int = 0
	if cborInfo == varFollows {
		// no special capacity to allocate the slice to
	} else {
		makeLength = int(aux)
	}

	dva, err = rv.CreateArray(makeLength)
	if err != nil {
		return err
	}

	if cborInfo == varFollows {
		//log.Printf("var array")
		subc := []byte{0}
		var idx uint64 = 0
		for true {
			_, err = io.ReadFull(dec.rin, subc)
			if err != nil {
				log.Printf("error reading next byte for var text")
				return err
			}
			if subc[0] == 0xff {
				// Done
				break
			}
			subrv, err := dva.GetArrayValue(idx)
			if err != nil {
				return err
			}
			err = dec.innerDecodeC(subrv, subc[0])
			if err != nil {
				log.Printf("error decoding array subob")
				return err
			}
			err = dva.AppendArray(subrv)
			if err != nil {
				return err
			}
			idx++
		}
	} else {
		var i uint64
		for i = 0; i < aux; i++ {
			subrv, err := dva.GetArrayValue(i)
			if err != nil {
				return err
			}
			err = dec.DecodeAny(subrv)
			if err != nil {
				log.Printf("error decoding array subob")
				return err
			}
			err = dva.AppendArray(subrv)
			if err != nil {
				return err
			}
		}
	}

	return dva.EndArray()
}

func (dec *Decoder) decodeBignum(c byte) (*big.Int, error) {
	cborType := c & typeMask
	cborInfo := c & infoBits

	aux, err := dec.handleInfoBits(cborInfo)
	if err != nil {
		log.Printf("error in bignum handleInfoBits: %v", err)
		return nil, err
	}
	//log.Printf("bignum cborType %x cborInfo %d aux %x", cborType, cborInfo, aux)

	if cborType != cborBytes {
		return nil, fmt.Errorf("attempting to decode bignum but sub object is not bytes but type %x", cborType)
	}

	rawbytes := make([]byte, aux)
	_, err = io.ReadFull(dec.rin, rawbytes)
	if err != nil {
		return nil, err
	}

	bn := big.NewInt(0)
	littleBig := &big.Int{}
	d := &big.Int{}
	for _, bv := range rawbytes {
		d.Lsh(bn, 8)
		littleBig.SetUint64(uint64(bv))
		bn.Or(d, littleBig)
	}

	return bn, nil
}

func (r *reflectValue) SetBignum(x *big.Int) error {
	rv := r.v
	switch rv.Kind() {
	case reflect.Ptr:
		return newReflectValue(reflect.Indirect(rv)).SetBignum(x)
	case reflect.Interface:
		rv.Set(reflect.ValueOf(*x))
		return nil
	case reflect.Int32:
		if x.BitLen() < 32 {
			rv.SetInt(x.Int64())
			return nil
		} else {
			return fmt.Errorf("int too big for int32 target")
		}
	case reflect.Int, reflect.Int64:
		if x.BitLen() < 64 {
			rv.SetInt(x.Int64())
			return nil
		} else {
			return fmt.Errorf("int too big for int64 target")
		}
	default:
		return fmt.Errorf("cannot assign bignum into Kind=%s Type=%s %#v", rv.Kind().String(), rv.Type().String(), rv)
	}
}

func (r *reflectValue) SetBytes(buf []byte) error {
	rv := r.v
	switch rv.Kind() {
	case reflect.Ptr:
		return newReflectValue(reflect.Indirect(rv)).SetBytes(buf)
	case reflect.Interface:
		rv.Set(reflect.ValueOf(buf))
		return nil
	case reflect.Slice:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			rv.SetBytes(buf)
			return nil
		} else {
			return fmt.Errorf("cannot write []byte to k=%s %s", rv.Kind().String(), rv.Type().String())
		}
	case reflect.String:
		rv.Set(reflect.ValueOf(string(buf)))
		return nil
	default:
		return fmt.Errorf("cannot assign []byte into Kind=%s Type=%s %#v", rv.Kind().String(), "" /*rv.Type().String()*/, rv)
	}
}

func (r *reflectValue) SetUint(u uint64) error {
	rv := r.v
	switch rv.Kind() {
	case reflect.Ptr:
		if rv.IsNil() {
			if rv.CanSet() {
				rv.Set(reflect.New(rv.Type().Elem()))
				// fall through to set indirect below
			} else {
				return fmt.Errorf("trying to put uint into unsettable nil ptr")
			}
		}
		return newReflectValue(reflect.Indirect(rv)).SetUint(u)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if rv.OverflowUint(u) {
			return fmt.Errorf("value %d does not fit into target of type %s", u, rv.Kind().String())
		}
		rv.SetUint(u)
		return nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if (u == 0xffffffffffffffff) || rv.OverflowInt(int64(u)) {
			return fmt.Errorf("value %d does not fit into target of type %s", u, rv.Kind().String())
		}
		rv.SetInt(int64(u))
		return nil
	case reflect.Interface:
		rv.Set(reflect.ValueOf(u))
		return nil
	default:
		return fmt.Errorf("cannot assign uint into Kind=%s Type=%#v %#v", rv.Kind().String(), rv.Type(), rv)
	}
}
func (r *reflectValue) SetInt(i int64) error {
	rv := r.v
	switch rv.Kind() {
	case reflect.Ptr:
		return newReflectValue(reflect.Indirect(rv)).SetInt(i)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if rv.OverflowInt(i) {
			return fmt.Errorf("value %d does not fit into target of type %s", i, rv.Kind().String())
		}
		rv.SetInt(i)
		return nil
	case reflect.Interface:
		rv.Set(reflect.ValueOf(i))
		return nil
	default:
		return fmt.Errorf("cannot assign int into Kind=%s Type=%#v %#v", rv.Kind().String(), rv.Type(), rv)
	}
}
func (r *reflectValue) SetFloat32(f float32) error {
	rv := r.v
	switch rv.Kind() {
	case reflect.Ptr:
		return newReflectValue(reflect.Indirect(rv)).SetFloat32(f)
	case reflect.Float32, reflect.Float64:
		rv.SetFloat(float64(f))
		return nil
	case reflect.Interface:
		rv.Set(reflect.ValueOf(f))
		return nil
	default:
		return fmt.Errorf("cannot assign float32 into Kind=%s Type=%#v %#v", rv.Kind().String(), rv.Type(), rv)
	}
}
func (r *reflectValue) SetFloat64(d float64) error {
	rv := r.v
	switch rv.Kind() {
	case reflect.Ptr:
		return newReflectValue(reflect.Indirect(rv)).SetFloat64(d)
	case reflect.Float32, reflect.Float64:
		rv.SetFloat(d)
		return nil
	case reflect.Interface:
		rv.Set(reflect.ValueOf(d))
		return nil
	default:
		return fmt.Errorf("cannot assign float64 into Kind=%s Type=%#v %#v", rv.Kind().String(), rv.Type(), rv)
	}
}
func (r *reflectValue) SetNil() error {
	rv := r.v
	switch rv.Kind() {
	case reflect.Ptr:
		//return setNil(reflect.Indirect(rv))
		rv.Elem().Set(reflect.Zero(rv.Elem().Type()))
	case reflect.Interface:
		if rv.IsNil() {
			// already nil, okay!
			return nil
		}
		rv.Set(reflect.Zero(rv.Type()))
	default:
		log.Printf("setNil wat %s", rv.Type())
		rv.Set(reflect.Zero(rv.Type()))
	}
	return nil
}

func (r *reflectValue) SetBool(b bool) error {
	rv := r.v
	reflect.Indirect(rv).Set(reflect.ValueOf(b))
	return nil
}

func (r *reflectValue) SetString(xs string) error {
	rv := r.v
	// handle either concrete string or string* to nil
	deref := reflect.Indirect(rv)
	if !deref.CanSet() {
		rv.Set(reflect.ValueOf(&xs))
	} else {
		deref.Set(reflect.ValueOf(xs))
	}
	//reflect.Indirect(rv).Set(reflect.ValueOf(joined))
	return nil
}

func (r *reflectValue) CreateTag(aux uint64, decoder TagDecoder) (DecodeValue, interface{}, error) {
	if decoder != nil {
		target := decoder.DecodeTarget()
		return newReflectValue(reflect.ValueOf(target)), target, nil
	} else {
		target := &CBORTag{}
		target.Tag = aux
		return newReflectValue(reflect.ValueOf(&target.WrappedObject)), target, nil
	}
}

func (r *reflectValue) SetTag(code uint64, val DecodeValue, decoder TagDecoder, target interface{}) error {
	rv := r.v
	var err error
	if decoder != nil {
		target, err = decoder.PostDecode(target)
		if err != nil {
			return err
		}
	}
	reflect.Indirect(rv).Set(reflect.ValueOf(target))
	return nil
}

// copied from encoding/json/decode.go
// An InvalidUnmarshalError describes an invalid argument passed to Unmarshal.
// (The argument to Unmarshal must be a non-nil pointer.)
type InvalidUnmarshalError struct {
	Type reflect.Type
}

func (e *InvalidUnmarshalError) Error() string {
	if e.Type == nil {
		return "json: Unmarshal(nil)"
	}

	if e.Type.Kind() != reflect.Ptr {
		return "json: Unmarshal(non-pointer " + e.Type.String() + ")"
	}
	return "json: Unmarshal(nil " + e.Type.String() + ")"
}

type CBORTag struct {
	Tag           uint64
	WrappedObject interface{}
}

func (t *CBORTag) ToCBOR(w io.Writer, enc *Encoder) error {
	_, err := w.Write(EncodeInt(MajorTypeTag, t.Tag, nil))
	if err != nil {
		return err
	}

	return enc.Encode(t.WrappedObject)
}

type Encoder struct {
	out    io.Writer
	filter func(v interface{}) interface{}

	scratch []byte
}

// parse StructField.Tag.Get("json" or "cbor")
func fieldTagName(xinfo string) (string, bool) {
	if len(xinfo) != 0 {
		// e.g. `json:"field_name,omitempty"`, or same for cbor
		// TODO: honor 'omitempty' option
		jiparts := strings.Split(xinfo, ",")
		if len(jiparts) > 0 {
			fieldName := jiparts[0]
			if len(fieldName) > 0 {
				return fieldName, true
			}
		}
	}
	return "", false
}

// Return fieldname, bool; if bool is false, don't use this field
func fieldname(fieldinfo reflect.StructField) (string, bool) {
	if fieldinfo.PkgPath != "" {
		// has path to private package. don't export
		return "", false
	}
	fieldname, ok := fieldTagName(fieldinfo.Tag.Get("cbor"))
	if !ok {
		fieldname, ok = fieldTagName(fieldinfo.Tag.Get("json"))
	}
	if ok {
		if fieldname == "" {
			return fieldinfo.Name, true
		}
		if fieldname == "-" {
			return "", false
		}
		return fieldname, true
	}
	return fieldinfo.Name, true
}

// Write out an object to an io.Writer
func Encode(out io.Writer, ob interface{}) error {
	return NewEncoder(out).Encode(ob)
}

// Write out an object to a new byte slice
func Dumps(ob interface{}) ([]byte, error) {
	writeTarget := &bytes.Buffer{}
	writeTarget.Grow(20000)
	err := Encode(writeTarget, ob)
	if err != nil {
		return nil, err
	}
	return writeTarget.Bytes(), nil
}

type MarshallValue interface {
	// Convert the value to CBOR. Specific CBOR data (such as tags) can be written
	// on the io.Writer and more complex datatype can be written using the
	// Encoder.
	//
	// To Write a Tag value, a possible implementation would be:
	//
	//  w.Write(cbor.EncodeTag(6, tag_value))
	//  enc.Encode(tagged_value)
	//
	ToCBOR(w io.Writer, enc *Encoder) error
}

type SimpleMarshallValue interface {
	// Convert the value to CBOR. The object is responsible to convert to CBOR
	// in the correct format.
	ToCBOR(w io.Writer) error
}

type CBORValue []byte

func (v CBORValue) ToCBOR(w io.Writer) error {
	_, err := w.Write(v)
	return err
}

// Return new Encoder object for writing to supplied io.Writer.
//
// TODO: set options on Encoder object.
func NewEncoder(out io.Writer) *Encoder {
	return &Encoder{out, nil, make([]byte, 9)}
}

func (enc *Encoder) SetFilter(filter func(v interface{}) interface{}) {
	enc.filter = filter
}

func (enc *Encoder) Encode(ob interface{}) error {
	if enc.filter != nil {
		ob = enc.filter(ob)
	}

	if v, ok := ob.(MarshallValue); ok {
		return v.ToCBOR(enc.out, enc)
	} else if v, ok := ob.(SimpleMarshallValue); ok {
		return v.ToCBOR(enc.out)
	}

	switch x := ob.(type) {
	case int:
		return enc.writeInt(int64(x))
	case int8:
		return enc.writeInt(int64(x))
	case int16:
		return enc.writeInt(int64(x))
	case int32:
		return enc.writeInt(int64(x))
	case int64:
		return enc.writeInt(x)
	case uint:
		return enc.tagAuxOut(cborUint, uint64(x))
	case uint8: /* aka byte */
		return enc.tagAuxOut(cborUint, uint64(x))
	case uint16:
		return enc.tagAuxOut(cborUint, uint64(x))
	case uint32:
		return enc.tagAuxOut(cborUint, uint64(x))
	case uint64:
		return enc.tagAuxOut(cborUint, x)
	case float32:
		return enc.writeFloat(float64(x))
	case float64:
		return enc.writeFloat(x)
	case string:
		return enc.writeText(x)
	case []byte:
		return enc.writeBytes(x)
	case bool:
		return enc.writeBool(x)
	case nil:
		return enc.tagAuxOut(cbor7, uint64(cborNull))
	case big.Int:
		return fmt.Errorf("TODO: encode big.Int")
	}

	// If none of the simple types work, try reflection
	return enc.writeReflection(reflect.ValueOf(ob))
}

func (enc *Encoder) writeReflection(rv reflect.Value) error {
	if enc.filter != nil {
		rv = reflect.ValueOf(enc.filter(rv.Interface()))
	}

	if ! rv.IsValid() {
	   return enc.tagAuxOut(cbor7, uint64(cborNull))
	}

	if v, ok := rv.Interface().(MarshallValue); ok {
		return v.ToCBOR(enc.out, enc)
	} else if v, ok := rv.Interface().(SimpleMarshallValue); ok {
		return v.ToCBOR(enc.out)
	}

	var err error
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return enc.writeInt(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return enc.tagAuxOut(cborUint, rv.Uint())
	case reflect.Float32, reflect.Float64:
		return enc.writeFloat(rv.Float())
	case reflect.Bool:
		return enc.writeBool(rv.Bool())
	case reflect.String:
		return enc.writeText(rv.String())
	case reflect.Slice, reflect.Array:
		elemType := rv.Type().Elem()
		if elemType.Kind() == reflect.Uint8 {
			// special case, write out []byte
			return enc.writeBytes(rv.Bytes())
		}
		alen := rv.Len()
		err = enc.tagAuxOut(cborArray, uint64(alen))
		for i := 0; i < alen; i++ {
			err = enc.writeReflection(rv.Index(i))
			if err != nil {
				log.Printf("error at array elem %d", i)
				return err
			}
		}
		return nil
	case reflect.Map:
		err = enc.tagAuxOut(cborMap, uint64(rv.Len()))
		if err != nil {
			return err
		}

		dup := func(b []byte) []byte {
			out := make([]byte, len(b))
			copy(out, b)
			return out
		}

		keys := rv.MapKeys()
		buf := new(bytes.Buffer)
		encKeys := make([]cborKeyEntry, 0, len(keys))
		for _, krv := range keys {
			tempEnc := NewEncoder(buf)
			err := tempEnc.writeReflection(krv)
			if err != nil {
				log.Println("error encoding map key", err)
				return err
			}
			kval := dup(buf.Bytes())
			encKeys = append(encKeys, cborKeyEntry{
				val: kval,
				key: krv,
			})
			buf.Reset()
		}

		sort.Sort(cborKeySorter(encKeys))

		for _, ek := range encKeys {
			vrv := rv.MapIndex(ek.key)

			_, err := enc.out.Write(ek.val)
			if err != nil {
				log.Printf("error writing map key")
				return err
			}
			err = enc.writeReflection(vrv)
			if err != nil {
				log.Printf("error encoding map val")
				return err
			}
		}

		return nil
	case reflect.Struct:
		// TODO: check for big.Int ?
		numfields := rv.NumField()
		structType := rv.Type()
		usableFields := 0
		for i := 0; i < numfields; i++ {
			fieldinfo := structType.Field(i)
			_, ok := fieldname(fieldinfo)
			if !ok {
				continue
			}
			usableFields++
		}
		err = enc.tagAuxOut(cborMap, uint64(usableFields))
		if err != nil {
			return err
		}
		for i := 0; i < numfields; i++ {
			fieldinfo := structType.Field(i)
			fieldname, ok := fieldname(fieldinfo)
			if !ok {
				continue
			}
			err = enc.writeText(fieldname)
			if err != nil {
				return err
			}
			err = enc.writeReflection(rv.Field(i))
			if err != nil {
				return err
			}
		}
		return nil
	case reflect.Interface:
		//return fmt.Errorf("TODO: serialize interface{} k=%s T=%s", rv.Kind().String(), rv.Type().String())
		return enc.Encode(rv.Interface())
	case reflect.Ptr:
		if rv.IsNil() {
			return enc.tagAuxOut(cbor7, uint64(cborNull))
		}
		return enc.writeReflection(reflect.Indirect(rv))
	}

	return fmt.Errorf("don't know how to CBOR serialize k=%s t=%s", rv.Kind().String(), rv.Type().String())
}

type cborKeySorter []cborKeyEntry
type cborKeyEntry struct {
	val []byte
	key reflect.Value
}

func (cks cborKeySorter) Len() int { return len(cks) }
func (cks cborKeySorter) Swap(i, j int) {
	cks[i], cks[j] = cks[j], cks[i]
}

func (cks cborKeySorter) Less(i, j int) bool {
	a := keyBytesFromEncoded(cks[i].val)
	b := keyBytesFromEncoded(cks[j].val)
	switch {
	case len(a) < len(b):
		return true
	case len(a) > len(b):
		return false
	default:
		if bytes.Compare(a, b) < 0 {
			return true
		}
		return false
	}
}

func keyBytesFromEncoded(data []byte) []byte {
	cborInfo := data[0] & infoBits

	if cborInfo <= 23 {
		return data[1:]
	} else if cborInfo == int8Follows {
		return data[2:]
	} else if cborInfo == int16Follows {
		return data[3:]
	} else if cborInfo == int32Follows {
		return data[5:]
	} else if cborInfo == int64Follows {
		return data[9:]
	}
	panic("shouldnt actually hit this")
}

func (enc *Encoder) writeInt(x int64) error {
	if x < 0 {
		return enc.tagAuxOut(cborNegint, uint64(-1-x))
	}
	return enc.tagAuxOut(cborUint, uint64(x))
}

func (enc *Encoder) tagAuxOut(tag byte, x uint64) error {
	var err error
	if x <= 23 {
		// tiny literal
		enc.scratch[0] = tag | byte(x)
		_, err = enc.out.Write(enc.scratch[:1])
	} else if x < 0x0ff {
		enc.scratch[0] = tag | int8Follows
		enc.scratch[1] = byte(x & 0x0ff)
		_, err = enc.out.Write(enc.scratch[:2])
	} else if x < 0x0ffff {
		enc.scratch[0] = tag | int16Follows
		enc.scratch[1] = byte((x >> 8) & 0x0ff)
		enc.scratch[2] = byte(x & 0x0ff)
		_, err = enc.out.Write(enc.scratch[:3])
	} else if x < 0x0ffffffff {
		enc.scratch[0] = tag | int32Follows
		enc.scratch[1] = byte((x >> 24) & 0x0ff)
		enc.scratch[2] = byte((x >> 16) & 0x0ff)
		enc.scratch[3] = byte((x >> 8) & 0x0ff)
		enc.scratch[4] = byte(x & 0x0ff)
		_, err = enc.out.Write(enc.scratch[:5])
	} else {
		err = enc.tagAux64(tag, x)
	}
	return err
}
func (enc *Encoder) tagAux64(tag byte, x uint64) error {
	enc.scratch[0] = tag | int64Follows
	enc.scratch[1] = byte((x >> 56) & 0x0ff)
	enc.scratch[2] = byte((x >> 48) & 0x0ff)
	enc.scratch[3] = byte((x >> 40) & 0x0ff)
	enc.scratch[4] = byte((x >> 32) & 0x0ff)
	enc.scratch[5] = byte((x >> 24) & 0x0ff)
	enc.scratch[6] = byte((x >> 16) & 0x0ff)
	enc.scratch[7] = byte((x >> 8) & 0x0ff)
	enc.scratch[8] = byte(x & 0x0ff)
	_, err := enc.out.Write(enc.scratch[:9])
	return err
}

// Encode a CBOR integer unit. The first argument is the major type, the second
// argument is the integer value. The result is a byte array from 1 to 9 bytes
// depending on the size of the integer value.
//
// The major type (tag argument) must be an integer between 0 and 7 else this
// function panics
//
// If the third parameter is non nil, the slice is reused to construct the
// result to avoid a memory allocation. It should be a slice with a sufficient
// capacity.
func EncodeInt(tag byte, v uint64, buf []byte) []byte {
	switch {
	case v <= 23:
		// tiny literal
		return EncodeOpcode(tag, byte(v), buf)
	case 23 < v && v < 0x0ff:
		return EncodeInt8(tag, uint8(v), buf)
	case 0xff <= v && v < 0x0ffff:
		return EncodeInt16(tag, uint16(v), buf)
	case 0xffff <= v && v < 0x0ffffffff:
		return EncodeInt32(tag, uint32(v), buf)
	default:
		return EncodeInt64(tag, v, buf)
	}
}

func EncodeOpcode(tag byte, opcode byte, buf []byte) []byte {
	if tag > 7 {
		panic("Wrong tag value")
	}
	return append(buf[0:0],
		(tag<<5)|opcode,
	)
}

func EncodeInt8(tag byte, v uint8, buf []byte) []byte {
	if tag > 7 {
		panic("Wrong tag value")
	}
	return append(buf[0:0],
		(tag<<5)|int8Follows,
		byte(v&0xff),
	)
}

func EncodeInt16(tag byte, v uint16, buf []byte) []byte {
	if tag > 7 {
		panic("Wrong tag value")
	}
	return append(buf[0:0],
		(tag<<5)|int16Follows,
		byte((v>>8)&0xff),
		byte(v&0xff),
	)
}

func EncodeInt32(tag byte, v uint32, buf []byte) []byte {
	if tag > 7 {
		panic("Wrong tag value")
	}
	return append(buf[0:0],
		(tag<<5)|int32Follows,
		byte((v>>24)&0xff),
		byte((v>>16)&0xff),
		byte((v>>8)&0xff),
		byte(v&0xff),
	)
}

func EncodeInt64(tag byte, v uint64, buf []byte) []byte {
	if tag > 7 {
		panic("Wrong tag value")
	}
	return append(buf[0:0],
		(tag<<5)|int64Follows,
		byte((v>>56)&0xff),
		byte((v>>48)&0xff),
		byte((v>>40)&0xff),
		byte((v>>32)&0xff),
		byte((v>>24)&0xff),
		byte((v>>16)&0xff),
		byte((v>>8)&0xff),
		byte(v&0xff),
	)
}

func (enc *Encoder) writeText(x string) error {
	enc.tagAuxOut(cborText, uint64(len(x)))
	_, err := io.WriteString(enc.out, x)
	return err
}

func (enc *Encoder) writeBytes(x []byte) error {
	enc.tagAuxOut(cborBytes, uint64(len(x)))
	_, err := enc.out.Write(x)
	return err
}

func (enc *Encoder) writeFloat(x float64) error {
	return enc.tagAux64(cbor7, math.Float64bits(x))
}

func (enc *Encoder) writeBool(x bool) error {
	if x {
		return enc.tagAuxOut(cbor7, uint64(cborTrue))
	} else {
		return enc.tagAuxOut(cbor7, uint64(cborFalse))
	}
}
