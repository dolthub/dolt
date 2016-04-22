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
  Struct: NomsKind,
  Type: NomsKind,
  Unresolved: NomsKind,
  Package: NomsKind,
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
  Struct: 18,
  Type: 19,
  Unresolved: 20,
  Package: 21,
};

const kindToStringMap: { [key: number]: string } = Object.create(null);
kindToStringMap[Kind.Bool] = 'Bool';
kindToStringMap[Kind.Uint8] = 'Uint8';
kindToStringMap[Kind.Uint16] = 'Uint16';
kindToStringMap[Kind.Uint32] = 'Uint32';
kindToStringMap[Kind.Uint64] = 'Uint64';
kindToStringMap[Kind.Int8] = 'Int8';
kindToStringMap[Kind.Int16] = 'Int16';
kindToStringMap[Kind.Int32] = 'Int32';
kindToStringMap[Kind.Int64] = 'Int64';
kindToStringMap[Kind.Float32] = 'Float32';
kindToStringMap[Kind.Float64] = 'Float64';
kindToStringMap[Kind.String] = 'String';
kindToStringMap[Kind.Blob] = 'Blob';
kindToStringMap[Kind.Value] = 'Value';
kindToStringMap[Kind.List] = 'List';
kindToStringMap[Kind.Map] = 'Map';
kindToStringMap[Kind.Ref] = 'Ref';
kindToStringMap[Kind.Set] = 'Set';
kindToStringMap[Kind.Struct] = 'Struct';
kindToStringMap[Kind.Type] = 'Type';
kindToStringMap[Kind.Unresolved] = 'Unresolved';
kindToStringMap[Kind.Package] = 'Package';

export function kindToString(kind: number): string {
  return kindToStringMap[kind];
}

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
