/* @flow */

import React from 'react';
import LabelList from './label_list.js';
import type {LabelDelegate} from './label.js';

type Props = {
  title: string,
  items: Array<string>,
  selected: string,
  onChange: (selection: string) => void
};

export default function RadioList(props: Props) : React.Element {
  let delegate: LabelDelegate<string> = {
    getLabel(item: string) : string {
      return item;
    },
    isSelected(item: string) : boolean {
      return item === props.selected;
    },
    getColor() : string {
      return '';
    },
    onChange(item: string) : void {
      props.onChange(item);
    }
  };

  return <LabelList title={props.title} items={props.items} delegate={delegate}/>;
}
