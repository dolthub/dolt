// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import ReactDOM from 'react-dom';
import preloadImage from './preload-image.js';
import {searchToParams, paramsToSearch} from './params.js';
import Nav from './nav.js';
import Photo from './photo.js';
import type {PhotoSize} from './types.js';
import Viewport from './viewport.js';

const transitionDelay = '200ms';

type Props = {
  fullscreen: boolean,
  gridHeight: number,
  gridLeft: number,
  gridSize: PhotoSize,
  gridTop: number,
  gridWidth: number,
  nav: Nav,
  photo: Photo,
  url: string,
  viewport: Viewport,
};

type State = {
  size: PhotoSize,
  sizeIsBest: boolean,
  url: string,
};

export default class PhotoGridItem extends React.Component<void, Props, State> {
  state: State;
  _parentTop: number;
  _parentLeft: number;
  _shouldTransition: boolean;

  constructor(props: Props) {
    super(props);
    this.state = {
      size: props.gridSize,
      sizeIsBest: false,
      url: props.url,
    };
    this._parentTop = 0;
    this._parentLeft = 0;
    this._shouldTransition = true;
  }

  componentWillUpdate() {
    const rect = ReactDOM.findDOMNode(this).parentElement.getBoundingClientRect();
    this._parentTop = rect.top;
    this._parentLeft = rect.left;
  }

  render(): React.Element<*> {
    const {fullscreen, photo, viewport} = this.props;
    const {sizeIsBest, url} = this.state;

    let clipStyle, imgStyle, overlay;
    if (fullscreen) {
      clipStyle = this._getClipFullscreenStyle();
      imgStyle = this._getImgFullscreenStyle();
      overlay = <div style={{
        animation: `fadeIn ${transitionDelay}`,
        backgroundColor: 'black',
        position: 'fixed',
        top: 0, right: 0, bottom: 0, left: 0,
        zIndex: 1, // above photo grid, below fullscreen img because it's before in the DOM
      }}/>;
      if (!sizeIsBest) {
        // The fullscreen image is low-res, fetch the hi-res version.
        // disabled to avoid glaring errors, though it's still not perfect - if an image loads in
        // less than transitionDelay then it will look a bit wonky.
        // TODO: Also fade in the hi-res image.
        const [bestSize, bestUrl] = photo.getBestSize(viewport.clientWidth, viewport.clientHeight);
        preloadImage(bestUrl).then(() => {
          this._shouldTransition = false;
          this.setState({size: bestSize, sizeIsBest: true, url: bestUrl});
          window.requestAnimationFrame(() => {
            this._shouldTransition = true;
          });
        });
      }
    } else {
      clipStyle = this._getClipGridStyle();
      imgStyle = this._getImgGridStyle();
    }

    return <div onClick={() => this._handleOnClick()}>
      {overlay}
      <div style={clipStyle}>
        <img data-id={photo.nomsPhoto.id} src={url} style={imgStyle}/>
      </div>
    </div>;
  }

  _getClipGridStyle(): {[key: string]: any} {
    const {gridTop, gridLeft, gridWidth, gridHeight} = this.props;
    const {size} = this.state;
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

  _getClipFullscreenStyle(): {[key: string]: any} {
    // We'll scale the image, not the surrounding div, which is used by the grid to clip the image.
    const {viewport} = this.props;
    const {size} = this.state;

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

  _getImgGridStyle(): {[key: string]: any} {
    const {gridWidth, gridHeight} = this.props;
    const {size} = this.state;
    const widthScale = gridWidth / size.width;
    const heightScale = gridHeight / size.height;

    // Reverse the width scale that the outer div was scaled by, then scale the image's width by how
    // the outer div's height was scaled (so that the proportions are correct).
    return {
      transition: this._maybeTransition(`transform ${transitionDelay} ease`),
      transform: `scale3d(${heightScale / widthScale}, 1, 1)`,
    };
  }

  _getImgFullscreenStyle(): {[key: string]: any} {
    return {
      transition: this._maybeTransition(`transform ${transitionDelay} ease`),
    };
  }

  _maybeTransition(transition: string): string {
    return this._shouldTransition === true ? transition : '';
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
