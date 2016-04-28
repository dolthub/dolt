// @flow

export type primitive =
    number |
    string |
    boolean;

export function isPrimitive(v: any): boolean {
  switch (typeof v) {
    case 'string':
    case 'number':
    case 'boolean':
      return true;
    default:
      return false;
  }
}
