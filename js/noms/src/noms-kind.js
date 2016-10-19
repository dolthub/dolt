// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export type NomsKind = number;

export const Kind: {
  Bool: NomsKind,
  Number: NomsKind,
  String: NomsKind,
  Blob: NomsKind,
  Value: NomsKind,
  List: NomsKind,
  Map: NomsKind,
  Ref: NomsKind,
  Set: NomsKind,
  Struct: NomsKind,
  Type: NomsKind,
  Cycle: NomsKind,
  Union: NomsKind,
} = {
  Bool: 0,
  Number: 1,
  String: 2,
  Blob: 3,
  Value: 4,
  List: 5,
  Map: 6,
  Ref: 7,
  Set: 8,
  Struct: 9,
  Type: 10,
  Cycle: 11,  // Only used in encoding/decoding.
  Union: 12,
};

const kindToStringMap: { [key: number]: string } = Object.create(null);
kindToStringMap[Kind.Bool] = 'Bool';
kindToStringMap[Kind.Number] = 'Number';
kindToStringMap[Kind.String] = 'String';
kindToStringMap[Kind.Blob] = 'Blob';
kindToStringMap[Kind.Value] = 'Value';
kindToStringMap[Kind.List] = 'List';
kindToStringMap[Kind.Map] = 'Map';
kindToStringMap[Kind.Ref] = 'Ref';
kindToStringMap[Kind.Set] = 'Set';
kindToStringMap[Kind.Struct] = 'Struct';
kindToStringMap[Kind.Type] = 'Type';
kindToStringMap[Kind.Cycle] = 'Cycle';
kindToStringMap[Kind.Union] = 'Union';

export function kindToString(kind: NomsKind): string {
  return kindToStringMap[kind];
}

export function isPrimitiveKind(k: NomsKind): boolean {
  switch (k) {
    case Kind.Bool:
    case Kind.Number:
    case Kind.String:
    case Kind.Blob:
    case Kind.Value:
    case Kind.Type:
      return true;
    default:
      return false;
  }
}
