// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import {notNull} from '@attic/noms';
import Nav from './nav.js';
import Photo, {createPhoto} from './photo.js';
import PhotoGridItem from './photo-grid-item.js';
import PhotoSetIterator, {EmptyIterator} from './photo-set-iterator.js';
import Viewport from './viewport.js';

const maxPhotoHeight = 300;
const photosPerPage = 10;
const photoSpacing = 5;

type Props = {
  availWidth: number,
  photo: ?Photo,
  photosIter: PhotoSetIterator,
  nav: Nav,
  viewport: Viewport,
};

type State = {
  photos: Photo[],
  photosIter: PhotoSetIterator,
  photosIterDone: boolean,
};

export default class PhotoGrid extends React.Component<void, Props, State> {
  state: State;
  _isMounted: boolean;
  _handleScrollToBottom: Function;

  constructor(props: Props) {
    super(props);
    this.state = {
      photos: [],
      photosIter: new EmptyIterator(),
      photosIterDone: true,
    };
    this._isMounted = false;
    this._handleScrollToBottom = () => {
      this._getMorePhotos(this.state.photos);
    };
  }

  componentDidMount() {
    this.props.viewport.addScrollToBottomListener(this._handleScrollToBottom);
    this._isMounted = true;
  }

  componentWillUnmount() {
    this.props.viewport.removeScrollToBottomListener(this._handleScrollToBottom);
    this._isMounted = false;
  }

  render(): React.Element<any> {
    const {availWidth, nav, photo, viewport} = this.props;
    const {photos, photosIter, photosIterDone} = this.state;

    if (photo) {
      // If the fullscreen photo is in the list of photos, zoom it in (this happens below).
      // Otherwise, show it immediately and don't load any others.
      const found = photos.find(p => p.equals(photo)) !== undefined;
      if (!found) {
        const [bestSize, bestUrl] = photo.getBestSize(viewport.clientWidth, viewport.clientHeight);
        return <PhotoGridItem
          fullscreen={true}
          gridHeight={0}
          gridLeft={0}
          gridSize={bestSize}
          gridTop={0}
          gridWidth={viewport.clientWidth}
          nav={nav}
          photo={photo}
          url={bestUrl}
          viewport={viewport}/>;
      }
    }

    if (photosIter !== this.props.photosIter) {
      this._getMorePhotos([]);
    }

    if (photos.length === 0) {
      // TODO: Distinguish between loading and no photos.
      return <div>No photos.</div>;
    }

    const row = [];
    const children = [];
    let top = 0;
    let right = 0;

    const finalizeRow = () => {
      const overflowTotal = Math.max(availWidth, right) - availWidth;
      let left = 0;
      for (const p of row) {
        let w = p.scaledWidth - overflowTotal * (p.scaledWidth / right);
        if (row.length > 0) {
          w -= photoSpacing;
        }
        children.push(<PhotoGridItem
          fullscreen={!!photo && p.photo.equals(photo)}
          gridHeight={maxPhotoHeight}
          gridLeft={left}
          gridSize={p.gridSize}
          gridTop={top}
          gridWidth={w}
          key={p.photo.nomsPhoto.hash.toString()}
          nav={nav}
          photo={p.photo}
          url={p.url}
          viewport={viewport}
        />);
        left += w;
        left += photoSpacing;
      }
      top += maxPhotoHeight;
      top += photoSpacing;
    };

    for (const photo of photos) {
      const [gridSize, url] = photo.getBestSize(0, maxPhotoHeight);
      const scaledWidth = (maxPhotoHeight / gridSize.height) * gridSize.width;
      row.push({gridSize, photo, scaledWidth, url});
      right += scaledWidth;
      if (right >= availWidth) {
        finalizeRow();
        row.length = 0;
        right = 0;
      }
    }

    // There may be photos in |row| that haven't been renderered yet. If possible, don't show them
    // because an incomplete row looks bad, and the infinite scrolling will eventually show them.
    // However, if there are no more photos to scroll, show them now.
    if (row.length > 0 && photosIterDone) {
      finalizeRow();
    }

    // Keep rendering photos until the page has filled, or there are no more photos.
    if (top < viewport.clientHeight && !photosIterDone) {
      this._getMorePhotos(photos);
    }

    return <div style={{position: 'relative', height: top}}>{children}</div>;
  }

  async _getMorePhotos(current: Photo[]): Promise<void> {
    const {photosIter} = this.props;
    const moreP = [];
    let next;
    while (!(next = await photosIter.next()).done && moreP.length < photosPerPage) {
      const [negDate, nomsPhoto] = notNull(next.value);
      const hash = nomsPhoto.hash.toString();
      const path = `.byDate[${negDate}][#${hash}]`;
      moreP.push(createPhoto(path, nomsPhoto));
    }

    const more = await Promise.all(moreP);

    if (this._isMounted) {
      this.setState({
        photos: current.concat(more),
        photosIter,
        photosIterDone: next.done,
      });
    }
  }
}
