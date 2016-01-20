// @flow

import eq from './eq.js';
import React from 'react';
import type {ChunkStore, Ref, NomsMap, Struct} from 'noms';
import {readValue} from 'noms';

type DefaultProps = {
  onLoad: () => void,
};

type Props = {
  onLoad: () => void,
  photoRef: Ref,
  style: Object,
  store: ChunkStore
};

type State = {
  photo: ?Struct,
  sizes: Array<{size: Struct, url: string}>
};

export default class Photo extends React.Component<DefaultProps, Props, State> {
  static defaultProps: DefaultProps;

  constructor(props: Props) {
    super(props);
    this.state = {
      photo: null,
      sizes: [],
    };
  }

  shouldComponentUpdate(nextProps: Props, nextState: State) : boolean {
    return !eq(nextProps, this.props) || !eq(nextState, this.state);
  }

  async _updatePhoto(props: Props) : Promise<void> {
    function area(size: Struct) : number {
      return size.get('Width') * size.get('Height');
    }

    const photo: Struct = await readValue(props.photoRef, props.store);

    // Sizes is a Map(Size, String) where the string is a URL.
    const sizes = [];
    const s: NomsMap<Struct, string> = photo.get('Sizes');
    await s.forEach((url, size) => {
      sizes.push({size, url});
    });
    sizes.sort((a, b) => area(a.size) - area(b.size));
    this.setState({photo, sizes});
  }

  render() : ?React.Element {
    this._updatePhoto(this.props);

    if (!this.state.photo || this.state.sizes.length === 0) {
      return null;
    }

    return (
      <img
        style={this.props.style}
        src={this.getURL()}
        onLoad={this.props.onLoad}/>
    );
  }

  getURL() : string {
    // If there are some remote URLs we can use, just pick the most appropriate size. We need the
    // smallest one that is bigger than our current dimensions.
    const sizes = this.state.sizes;
    const w = this.props.style.width || 0;
    const h = this.props.style.height || 0;
    const size = sizes.find(({size}) => {
      return size.get('Width') >= w && size.get('Height') >= h;
    });
    return size ? size.url : sizes[sizes.length - 1].url;
  }
}

Photo.defaultProps = {
  onLoad() {},
};
