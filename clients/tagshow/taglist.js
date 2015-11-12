/* @flow */

'use strict';

import React from 'react';

const tagStyle = {
  display: 'block',
  margin: '3px',
  marginRight: '25px',
  whiteSpace: 'nowrap'
};

type DefaultProps = {};

type Props = {
  selected: Set<string>,
  tags: Array<string>,
  onChange: (selected: Set<string>) => void
};

type State = {};

export default class TagList extends React.Component<DefaultProps, Props, State> {
  handleChange(tag: string) {
    let selected = this.props.selected;
    selected.has(tag) ? selected.delete(tag) : selected.add(tag);
    this.props.onChange(new Set(selected));
  }

  render() : React.Element {
    let tags = [...this.props.tags].sort();
    let labels = tags.map(tag => {
      return <label style={tagStyle} key={tag}>
        <input type="checkbox" name="tc"
          checked={this.props.selected.has(tag)}
          onChange={() => this.handleChange(tag) }/>
        {tag}
      </label>;
    });

    return <div>{labels}</div>;
  }
}
