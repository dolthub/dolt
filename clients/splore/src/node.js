// @flow

import classNames from 'classnames';
import React from 'react';
import {Ref} from 'noms';
import nomsServer from './noms_server.js';

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
  nomsRef: ?Ref,
  onClick: (e: Event, s: String) => void,
};

type State = {
  x: number,
  y: number,
};

export default class Node extends React.Component<void, Props, State> {
  constructor(props: Props) {
    super(props);

    this.state = {
      x: this.props.fromX,
      y: this.props.fromY,
    };
  }

  render(): React.Element {
    if (this.state.x !== this.props.x ||
        this.state.y !== this.props.y) {
      window.requestAnimationFrame(() => this.setState({
        x: this.props.x,
        y: this.props.y,
      }));
    }

    let textAnchor = 'start';
    let textX = 10;
    const translate = `translate3d(${this.state.x}px, ${this.state.y}px, 0)`;

    if (this.props.canOpen) {
      textAnchor = 'end';
      textX = -10;
    }

    let text = this.props.text;
    if (this.props.nomsRef) {
      const url = `${nomsServer}/ref/${this.props.nomsRef.toString()}`;
      text = <a xlinkHref={url}>{text}</a>;
    }
    return (
      <g className='node' onClick={this.props.onClick} style={{transform:translate}}>
        {this.getShape()}
        <text x={textX} dy='.35em' textAnchor={textAnchor}>
          {text}
        </text>
        <title>{this.props.title}</title>
      </g>
    );
  }

  getShape() : React.Element {
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
