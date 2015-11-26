/* @flow */

import React from 'react';
import type {DataArray} from './data.js';
import LabelList from './label_list.js';
import type {DataEntry} from './data.js';
import type {LabelDelegate} from './label.js';

type Props = {
  title: string,
  data: DataArray,
  selected: Set<DataEntry>,
  onChange: (selection: Set<DataEntry>) => void
};

export default function SeriesList(props: Props) : React.Element {
  let {selected} = props;
  let delegate: LabelDelegate<DataEntry> = {
    getLabel(item: DataEntry) : string {
      return item.key;
    },
    isSelected(item: DataEntry) : boolean {
      return selected.has(item);
    },
    getColor(item: DataEntry) : string {
      return item.color || '';
    },
    onChange(item: DataEntry) : void {
      selected = new Set(selected);
      selected.has(item) ? selected.delete(item) : selected.add(item);
      props.onChange(selected);
    }
  };

  return <LabelList title={props.title} items={props.data} delegate={delegate}/>;
}
