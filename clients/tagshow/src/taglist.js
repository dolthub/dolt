// @flow

import React from 'react';

const tagStyle = {
  display: 'block',
  margin: '3px',
  marginRight: '25px',
  whiteSpace: 'nowrap',
};

type Props = {
  selected: Set<string>,
  tags: Array<string>,
  onChange: (selected: Set<string>) => void,
};

function handleChange(props: Props, tag: string) {
  const selected = new Set(props.selected);
  selected.has(tag) ? selected.delete(tag) : selected.add(tag);
  props.onChange(selected);
}

export default function TagList(props: Props) : React.Element {
  const tags = [...props.tags].sort();
  const labels = tags.map(tag => {
    return <label style={tagStyle} key={tag}>
      <input type="checkbox" name="tc"
        checked={props.selected.has(tag)}
        onChange={() => handleChange(props, tag) }/>
      {tag}
    </label>;
  });

  return <div>{labels}</div>;
}
