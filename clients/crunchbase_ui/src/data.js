/* @flow */

export type DataPoint = {x: number, y: number};
export type DataEntry = {values: Array<DataPoint>, key: string, color?: string};
export type DataArray = Array<DataEntry>;

export default function getAllData() : DataArray {
  let seed = [];
  let seriesA = [];
  let seriesB = [];
  for (let i = 0; i < 24; i++) {
    seed.push({x: i, y: i});
    seriesA.push({x: i, y: i});
    seriesB.push({x: i, y: i});
  }
  return [
    {
      values: seed,
      key: 'Seed',
      color: '#011f4b'
    },
    {
      values: seriesA,
      key: 'A',
      color: '#03396c'
    },
    {
      values: seriesB,
      key: 'B',
      color: '#005b96'
    }
  ];
}
