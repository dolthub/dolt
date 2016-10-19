// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import classNames from 'classnames';
import React from 'react';
import {Hash} from '@attic/noms';

type Props = {
  canOpen: boolean,
  isOpen: boolean,
  shape: string,
  text: string,
  title: string,
  fromX: number,
  fromY: number,
  x: number,
  y: number,
  spaceX: number,
  hash: ?Hash,
  db: string,
  onClick: (e: MouseEvent, s: String) => void,
};

type State = {
  x: number,
  y: number,
};

export default class Node extends React.Component<void, Props, State> {
  state: State;

  constructor(props: Props) {
    super(props);

    this.state = {
      x: this.props.fromX,
      y: this.props.fromY,
    };
  }

  render(): React.Element<any> {
    if (this.state.x !== this.props.x ||
        this.state.y !== this.props.y) {
      window.requestAnimationFrame(() => this.setState({
        x: this.props.x,
        y: this.props.y,
      }));
    }

    const translate = `translate3d(${this.state.x}px, ${this.state.y}px, 0)`;

    let text = this.props.text;
    if (this.props.hash) {
      const url = `/?db=${this.props.db}&p=#${this.props.hash.toString()}`;
      text = <a href={url}>{text}</a>;
    }

    const foreignObjStyle = {
      overflow: 'visible', // Firefox like
    };

    const paraStyle = {
      overflow: 'hidden',
      textAlign: 'right',
      textOverflow: 'ellipsis',
      whiteSpace: 'nowrap',
      fontSize: '11px',
      fontFamily: '"Menlo", monospace',
    };

    return (
      <g className='node' onClick={this.props.onClick} style={{transform:translate}}>
        {this.getShape()}
        <foreignObject style={foreignObjStyle} x={-this.props.spaceX + 10} y='-.35em'
          width={this.props.spaceX - 20} height='0.7em'>
          <div title={this.props.title || this.props.text} style={paraStyle}>{text}</div>
        </foreignObject>
      </g>
    );
  }

  getShape() : React.Element<any> {
    const className = classNames('icon', {open:this.props.isOpen});
    switch (this.props.shape) {
      case 'circle':
        return <circle className={className} r='4.5'/>;
      case 'rect':
        // rx:1.35 and ry:1.35 for rounded corners, but not doing until I learn how to make the/
        // triangle match below.
        return <rect className={className} x='-4.5' y='-4.5' width='9' height='9'/>;
      case 'triangle':
        return <polygon className={className} points='0,-4.5 4.5,4.5 -4.5,4.5' rx='1.35'
            ry='1.35'/>;
    }
    throw new Error('unreachable');
  }
}
