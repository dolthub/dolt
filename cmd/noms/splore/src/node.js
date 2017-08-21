// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React, {Component, Element} from 'react';

type Props = {
  hasChildren: boolean,
  isOpen: boolean,
  text: string,
  fromX: number,
  fromY: number,
  x: number,
  y: number,
  spaceX: number,
  onClick: (e: MouseEvent, s: string) => any,
};

type State = {
  x: number,
  y: number,
};

export default class Node extends Component<void, Props, State> {
  state: State;

  constructor(props: Props) {
    super(props);

    this.state = {
      x: this.props.fromX,
      y: this.props.fromY,
    };
  }

  render(): Element<any> {
    const {hasChildren, isOpen, onClick, text, x, y} = this.props;

    if (this.state.x !== x || this.state.y !== y) {
      window.requestAnimationFrame(() => this.setState({x, y}));
    }

    const gStyle = {
      transition: 'transform 200ms',
      transform: `translate3d(${this.state.x}px, ${this.state.y}px, 0)`,
    };

    const circleStyle = {
      cursor: hasChildren ? (isOpen ? 'zoom-out' : 'zoom-in') : 'default',
      fill: hasChildren && !isOpen ? 'rgb(176, 196, 222)' : 'white',
      stroke: 'steelblue',
      strokeWidth: '1.5px',
    };

    const foreignObjStyle = {
      overflow: 'visible', // Firefox like
    };

    const paraStyle = {
      fontFamily: '"Menlo", monospace',
      fontSize: '11px',
      overflow: 'hidden',
      textAlign: 'right',
      textOverflow: 'ellipsis',
      whiteSpace: 'nowrap',
    };

    const spanStyle = {
      backgroundColor: 'rgba(255, 255, 255, 0.7)',
    };

    return (
      <g onClick={onClick} style={gStyle}>
        <circle style={circleStyle} r="4.5" />
        <foreignObject
          style={foreignObjStyle}
          x={-this.props.spaceX + 10}
          y="-.35em"
          width={this.props.spaceX - 20}
          height="0.7em"
        >
          <div title={text} style={paraStyle}>
            <span style={spanStyle}>
              {text}
            </span>
          </div>
        </foreignObject>
      </g>
    );
  }
}
