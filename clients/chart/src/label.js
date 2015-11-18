/* @flow */

import React from 'react';

type Props<T> = {
  item: T,
  delegate: LabelDelegate<T>
};

export type LabelDelegate<T> = {
  getLabel: (item: T) => string,
  isSelected: (item: T) => boolean,
  getColor: (item: T) => string,
  onChange: (item: T) => void
};

export default function Label(props: Props) : React.Element {
  let {delegate, item} = props;
  return <label><input type="checkbox"
      checked={delegate.isSelected(item)}
      onChange={() => delegate.onChange(item)}/>
        <span style={{color: delegate.getColor(item)}}/>
        <span>{delegate.getLabel(item)}</span>
      </label>;
}
