// @flow

import React from 'react';

type Props<T> = {
  delegate: ListDelegate<T>,
  items: Array<T>,
  title?: string
};

export default function List<T>(props: Props<T>) : React.Element {
  const delegate = props.delegate;
  const labels = props.items.map(item => {
    return <Label key={delegate.getLabel(item)} item={item}
        delegate={delegate}/>;
  });
  return <div className='selection-list'>
    {props.title ? <h3>{props.title}</h3> : null}
    {labels}
  </div>;
}

type LabelProps<T> = {
  item: T,
  delegate: ListDelegate<T>
};

export type ListDelegate<T> = {
  getLabel: (item: T) => string,
  isSelected: (item: T) => boolean,
  getColor?: (item: T) => string,
  onChange: (item: T) => void
};

function Label(props: LabelProps) : React.Element {
  const {delegate, item} = props;
  return <label><input type="checkbox"
      checked={delegate.isSelected(item)}
      onChange={() => delegate.onChange(item)}/>
        <span style={{color: delegate.getColor && delegate.getColor(item)}}/>
        <span>{delegate.getLabel(item)}</span>
      </label>;
}
