/* @flow */

'use strict';

import Photo from './photo.js';
import React from 'react';
import type {ChunkStore, Ref} from 'noms';

const containerStyle = {
  position: 'absolute',
  left: 0,
  top: 0,
  width: '100%',
  height: '100%',
  overflow: 'hidden',

  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center'
};

type DefaultProps = {};

type Props = {
  store: ChunkStore,
  photos: Array<Ref>
};

type State = {
  index: number
};

export default class SlideShow extends React.Component<DefaultProps, Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      index: 0
    };
  }

  handleTimeout() {
    let index = this.state.index + 1;
    if (index >= this.props.photos.length) {
      index = 0;
    }
    this.setState({index});
  }

  render() : ?React.Element {
    let photoRef = this.props.photos[this.state.index];
    if (!photoRef) {
      return null;
    }

    return (
      <div style={containerStyle}>
        <Item
          photoRef={photoRef}
          store={this.props.store}
          onTimeout={() => this.handleTimeout()} />
      </div>
    );
  }
}

type ItemProps = {
    onTimeout: () => void,
    photoRef: Ref,
    store: ChunkStore
};

type ItemState = {
  timerId: number
};

class Item extends React.Component<DefaultProps, ItemProps, ItemState> {
  constructor(props: ItemProps) {
    super(props);
    this.state = {
      timerId: 0
    };
  }

  setTimeout() {
    this.setState({
      timerId: window.setTimeout(this.props.onTimeout, 3000)
    });
  }

  componentWillUnmount() {
    window.clearTimeout(this.state.timerId);
  }

  render() : React.Element {
    let style = {
      objectFit: 'contain',
      width: window.innerWidth,
      height: window.innerHeight
    };

    return (
      <Photo
        store={this.props.store}
        onLoad={() => this.setTimeout()}
        photoRef={this.props.photoRef}
        style={style}/>
    );
  }
}
