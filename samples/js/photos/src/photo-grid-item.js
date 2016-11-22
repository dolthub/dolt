// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import ReactDOM from 'react-dom';
import {searchToParams, paramsToSearch} from './params.js';
import Nav from './nav.js';
import Photo from './photo.js';
import type {PhotoSize} from './types.js';
import Viewport from './viewport.js';
import {
  Blob as NomsBlob,
  Ref,
  Database,
} from '@attic/noms';

const transitionDelay = '200ms';

type Props = {
  db: Database,
  fullscreen: boolean,
  gridHeight: number,
  gridLeft: number,
  gridSize: PhotoSize,
  gridTop: number,
  gridWidth: number,
  nav: Nav,
  photo: Photo,
  viewport: Viewport,
};

type State = {
  // The NomsBlob we are either currently displaying, or loading in preparation to display. When
  // this changes, we start loading it asynchronously and eventually change |url| to match once it
  // has been loaded.
  blob: Ref<NomsBlob> | null,

  // A blob: URL referring to the photo we are actually currently displaying. Once |blob| has been
  // loaded, this is changed to refer to it.
  url: string | null,
};

export default class PhotoGridItem extends React.Component<void, Props, State> {
  state: State;
  _parentTop: number;
  _parentLeft: number;
  _shouldTransition: boolean;
  _didUnmount: boolean;

  constructor(props: Props) {
    super(props);
    this.state = {
      blob: this._getBlob(props),
      url: null,
    };
    this._parentTop = 0;
    this._parentLeft = 0;
    this._didUnmount = false;
    this._load(this.state.blob);
  }

  componentWillReceiveProps(nextProps: Props) {
    const nextBlob = this._getBlob(nextProps);
    if (nextBlob === this.state.blob) {
      return;
    }
    this.setState({
      blob: nextBlob,
    });
    this._load(nextBlob);
  }

  componentWillUpdate(nextProps: Props, nextState: State) {
    if (this.state.url !== nextState.url) {
      this._releaseBlobURL();
    }

    const rect = ReactDOM.findDOMNode(this).parentElement.getBoundingClientRect();
    this._parentTop = rect.top;
    this._parentLeft = rect.left;
  }

  _getBlob(props: Props): Ref<NomsBlob> {
    const [w, h] = props.fullscreen ?
      [props.viewport.clientWidth, props.viewport.clientHeight] :
      [props.gridSize.width, props.gridSize.height];
    const [, blob] = props.photo.getBestSize(w, h);
    return blob;
  }

  async _load(blob: Ref<NomsBlob>) {
    const b = await blob.targetValue(this.props.db);
    const r = b.getReader();
    const parts = [];
    for (;;) {
      const n = await r.read();

      if (n.done) {
        break;
      }

      // might have changed
      if (this.state.blob !== blob) {
        return;
      }

      // might have unloaded
      if (this._didUnmount) {
        return;
      }

      parts.push(n.value);
    }
    this.setState({
      url: URL.createObjectURL(new Blob(parts)),
    });
  }

  componentWillUnmount() {
    this._releaseBlobURL();
    this._didUnmount = true;
  }

  _releaseBlobURL() {
    if (this.state.url) {
      URL.revokeObjectURL(this.state.url);
    }
  }

  render(): React.Element<*> {
    const {fullscreen, photo, viewport} = this.props;
    let clipStyle, imgStyle, overlay;
    const {url} = this.state;

    if (fullscreen) {
      const [bestSize] = photo.getBestSize(viewport.clientWidth, viewport.clientHeight);
      clipStyle = this._getClipFullscreenStyle(bestSize);
      imgStyle = this._getImgFullscreenStyle(bestSize);
      overlay = <div style={{
        animation: `fadeIn ${transitionDelay}`,
        backgroundColor: 'black',
        position: 'fixed',
        top: 0, right: 0, bottom: 0, left: 0,
        zIndex: 1, // above photo grid, below fullscreen img because it's before in the DOM
      }}/>;
    } else {
      clipStyle = this._getClipGridStyle(this.props.gridSize);
      imgStyle = this._getImgGridStyle(this.props.gridSize);
    }

    return <div onClick={() => this._handleOnClick()}>
      {overlay}
      <div style={clipStyle}>
        <img data-id={photo.nomsPhoto.id} src={url} style={imgStyle}/>
      </div>
    </div>;
  }

  _getClipGridStyle(size: PhotoSize): {[key: string]: any} {
    const {gridTop, gridLeft, gridWidth, gridHeight} = this.props;
    const widthScale = gridWidth / size.width;
    const heightScale = gridHeight / size.height;

    return {
      overflow: 'hidden',
      position: 'absolute',
      transition: this._maybeTransition(`z-index ${transitionDelay} step-end,
                                         transform ${transitionDelay} ease`),
      // This is an awkward way to implement transformOrigin, but we can't use
      // it because it doesn't work well with transitions.
      transform: `translate3d(${-size.width / 2}px, ${-size.height / 2}px, 0)
                  scale3d(${widthScale}, ${heightScale}, 1)
                  translate3d(${(size.width / 2)}px, ${(size.height / 2)}px, 0)
                  translate3d(${gridLeft / widthScale}px, ${gridTop / heightScale}px, 0)`,
    };
  }

  _getClipFullscreenStyle(size: PhotoSize): {[key: string]: any} {
    // We'll scale the image, not the surrounding div, which is used by the grid to clip the image.
    const {viewport} = this.props;

    // Figure out whether we should be scaling width vs height to the dimensions of the screen.
    const widthScale = viewport.clientWidth / size.width;
    const heightScale = viewport.clientHeight / size.height;
    const scale = Math.min(widthScale, heightScale);

    // Transform the image to the center of the screen.
    const middleLeft = (viewport.clientWidth - size.width) / 2 - this._parentLeft;
    const middleTop = (viewport.clientHeight - size.height) / 2 - this._parentTop;

    return {
      position: 'absolute',
      transition: this._maybeTransition(`z-index ${transitionDelay} step-start,
                                         transform ${transitionDelay} ease`),
      // TODO: There appears to be a rounding error in here somewhere which causes the high-res
      // fullscreen image to be vertically ~1px low. Perhaps scaling before translating would help?
      transform: `translate3d(${middleLeft}px, ${middleTop}px, 0) scale3d(${scale}, ${scale}, 1)`,
      zIndex: 1,
    };
  }

  _getImgGridStyle(size: PhotoSize): {[key: string]: any} {
    const {gridWidth, gridHeight} = this.props;
    const widthScale = gridWidth / size.width;
    const heightScale = gridHeight / size.height;

    // Reverse the width scale that the outer div was scaled by, then scale the image's width by how
    // the outer div's height was scaled (so that the proportions are correct).
    return {
      transition: this._maybeTransition(`transform ${transitionDelay} ease`),
      transform: `scale3d(${heightScale / widthScale}, 1, 1)`,
    };
  }

  _getImgFullscreenStyle(size: PhotoSize): {[key: string]: any} {
    const t = this._maybeTransition(`transform ${transitionDelay} ease`);
    return {
      width: size.width,
      height: size.height,
      transition: t,
    };
  }

  _maybeTransition(_: string): string {
    // TODO: Make transitions work again.
    return '';
  }

  _handleOnClick() {
    const {fullscreen, nav, photo} = this.props;
    if (fullscreen && nav.from()) {
      nav.back();
      return;
    }

    const params = searchToParams(location.href);
    if (fullscreen) {
      params.delete('photo');
    } else {
      params.set('photo', photo.path);
    }
    nav.push(location.pathname + paramsToSearch(params));
  }
}
