/* @flow */

'use strict';

import React from 'react';
import Label from './label.js';
import type {LabelDelegate} from './label.js';

type Props<T> = {
  delegate: LabelDelegate<T>,
  items: Array<T>,
  title?: string
};

export default function LabelList<T>(props: Props<T>) : React.Element {
  let delegate = props.delegate;
  let labels = props.items.map(item => {
    return <Label key={delegate.getLabel(item)} item={item}
        delegate={delegate}/>;
  });
  return <div className='selection-list'>
    {props.title ? <h3>{props.title}</h3> : null}
    {labels}
  </div>;
}
