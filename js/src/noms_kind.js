// @flow

export type NomsKind = number;

export const Kind: {
  Bool: NomsKind,
  Uint8: NomsKind,
  Uint16: NomsKind,
  Uint32: NomsKind,
  Uint64: NomsKind,
  Int8: NomsKind,
  Int16: NomsKind,
  Int32: NomsKind,
  Int64: NomsKind,
  Float32: NomsKind,
  Float64: NomsKind,
  String: NomsKind,
  Blob: NomsKind,
  Value: NomsKind,
  List: NomsKind,
  Map: NomsKind,
  Ref: NomsKind,
  Set: NomsKind,
  Enum: NomsKind,
  Struct: NomsKind,
  Type: NomsKind,
  Unresolved: NomsKind,
  Package: NomsKind
} = {
  Bool: 0,
  Uint8: 1,
  Uint16: 2,
  Uint32: 3,
  Uint64: 4,
  Int8: 5,
  Int16: 6,
  Int32: 7,
  Int64: 8,
  Float32: 9,
  Float64: 10,
  String: 11,
  Blob: 12,
  Value: 13,
  List: 14,
  Map: 15,
  Ref: 16,
  Set: 17,
  Enum: 18,
  Struct: 19,
  Type: 20,
  Unresolved: 21,
  Package: 22
};

export function isPrimitiveKind(k: NomsKind): boolean {
  switch (k) {
    case Kind.Bool:
    case Kind.Int8:
    case Kind.Int16:
    case Kind.Int32:
    case Kind.Int64:
    case Kind.Float32:
    case Kind.Float64:
    case Kind.Uint8:
    case Kind.Uint16:
    case Kind.Uint32:
    case Kind.Uint64:
    case Kind.String:
    case Kind.Blob:
    case Kind.Value:
    case Kind.Type:
    case Kind.Package:
      return true;
    default:
      return false;
  }
}
