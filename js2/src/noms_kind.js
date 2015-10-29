/* @flow */

'use strict';

export type NomsKind = number;

export const Kind: {
  Bool: NomsKind,
  UInt8: NomsKind,
  UInt16: NomsKind,
  UInt32: NomsKind,
  UInt64: NomsKind,
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
  TypeRef: NomsKind,
  Unresolved: NomsKind,
  Package: NomsKind
} = {
  Bool: 0,
  UInt8: 1,
  UInt16: 2,
  UInt32: 3,
  UInt64: 4,
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
  TypeRef: 20,
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
    case Kind.UInt8:
    case Kind.UInt16:
    case Kind.UInt32:
    case Kind.UInt64:
    case Kind.String:
    case Kind.Blob:
    case Kind.Value:
    case Kind.TypeRef:
    case Kind.Package:
      return true;
    default:
      return false;
  }
}
