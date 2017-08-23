/*
CBOR is IETF RFC 7049, the "Concise Binary Object Representation"
http://tools.ietf.org/html/rfc7049

In can be thought of as "binary JSON" but is a superset and somewhat richer representation than JSON.

Other implementations and more information can also be found at:
http://cbor.io/

Serialization and deserialization of structs uses the same tag format as the encoding/json package. If different json and cbor serialization names are needed, a tag `cbor:"fieldName"` can be specified. Example:

  type DemoStruct struct {
    FieldNeedsDifferentName string `json:"serialization_name"`
    FieldNeedsJsonVsCborName int `json:"json_name" cbor:"cbor_name"`
  }

This might generate json:
{"serialization_name":"foo", "json_name":2}

And CBOR equivalent to:
{"serialization_name":"foo", "cbor_name":2}

*/
package cbor
