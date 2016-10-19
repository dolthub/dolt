// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import Nav from './nav.js';
import type {
  CountMap,
  Face,
  PhotoIndex,
  PhotoSize,
} from './types.js';
import Photo from './photo.js';
import PhotoGrid from './photo-grid.js';
import PhotoSetIterator, {
  EmptyIterator,
  PhotoSetIntersectionIterator,
  SinglePhotoSetIterator,
} from './photo-set-iterator.js';
import {
  Map as NomsMap,
  Set as NomsSet,
  invariant,
  notNull,
} from '@attic/noms';
import Viewport from './viewport.js';

const nanosInMillis = 1e6; // TODO: just provide conversion functions for Date
const panelWidth = 250;
const gutter = 5; // match photo spacing
const faceSize = 106;
const selectedFaceBorderWidth = 4;

type FaceState = {
  face: Face,
  size: PhotoSize,
  url: string,
};

type Props = {
  index: PhotoIndex,
  nav: Nav,
  photo: ?Photo,
  viewport: Viewport,
};

type State = {
  allFaces: FaceState[],
  allTags: string[],
  minDate: Date,
  maxDate: Date,
  photosIter: PhotoSetIterator,
  selectedDate: Date,
  selectedFaces: Set<string>,
  selectedTags: Set<string>,
};

const panelHeadStyle = {
  fontWeight: 400,
  marginBottom: '0.5em',
  opacity: 0.8,
};

const panelStyle = {
  background: '#eee',
  boxSizing: 'border-box',
  fontSize: '90%',
  marginRight: gutter,
  padding: '10px 15px',
  width: panelWidth,
};

const subPanelStyle = {
  marginBottom: '25px',
};

class BreakException extends Error {
}

export default class PhotosPage extends React.Component<void, Props, State> {
  state: State;
  _isMounted: boolean;

  // This promise is used to make sure there is only a single _setFilters call running at any time,
  // and that the last _setFilters call will eventually get executed.
  _settingFilters: Promise<void>;

  constructor(props: Props) {
    super(props);
    const now = new Date();
    this.state = {
      allFaces: [],
      allTags: [],
      minDate: now,
      maxDate: now,
      photosIter: new EmptyIterator(),
      selectedDate: now,
      selectedFaces: new Set(),
      selectedTags: new Set(),
    };
    this._isMounted = false;
    this._settingFilters = Promise.resolve();
  }

  componentDidMount() {
    this._isMounted = true;
    this._fetch();
  }

  componentWillUnmount() {
    this._isMounted = false;
  }

  render(): React.Element<*> {
    const {nav, photo, viewport} = this.props;
    const {selectedDate, minDate, maxDate, photosIter} = this.state;

    const photoGrid = <PhotoGrid
      availWidth={viewport.clientWidth - panelWidth - gutter}
      nav={nav}
      photo={photo}
      photosIter={photosIter}
      viewport={viewport}
    />;

    let faces = this._getFaces();
    if (faces && faces.length > 0) {
      faces = [
        <div key='who' style={panelHeadStyle}>Who</div>,
        <div key='faces' style={subPanelStyle}>{faces}</div>,
      ];
    }

    let tags = this._getTags();
    if (tags && tags.length > 0) {
      tags = [
        <div key='what' style={panelHeadStyle}>What</div>,
        <div key='tags' style={subPanelStyle}>{tags}</div>,
      ];
    }

    const newDateSpan = d => {
      const months =
        ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      return <span style={{whiteSpace: 'nowrap'}}>
        {`${months[d.getMonth()]} ${d.getDay() + 1}, ${d.getFullYear()}`}
      </span>;
    };

    const when = [
      <div key='when' style={panelHeadStyle}>When</div>,
      <input key='range'
             type='range'
             min={minDate.getTime()} max={maxDate.getTime()}
             defaultValue={selectedDate.getTime()}
             style={{margin: 0, width: '100%'}}
             onChange={e => this._maybeUpdateRange(e)} />,
      <div key='date'>{newDateSpan(minDate)} &ndash; {newDateSpan(maxDate)}</div>,
    ];

    return <div style={{display:'flex', alignItems:'flex-start'}}>
      <div style={panelStyle}>
        {faces}
        {tags}
        {when}
      </div>
      <div style={{flexGrow: 1}}>{photoGrid}</div>
    </div>;
  }

  _getTags(): React.Element<*>[] {
    return this.state.allTags.map(tag => <label key={tag} style={{display: 'block'}}>
      <input type='checkbox' onChange={e => this._toggleSelectedTag(e.target.checked, tag)}/>
      &nbsp;{tag}
    </label>);
  }

  _getFaces(): React.Element<*>[] {
    const {allFaces, selectedFaces} = this.state;

    const divStyle = {
      display: 'inline-block',
      height: faceSize,
      overflow: 'hidden',
      margin: 2,
      position: 'relative',
      verticalAlign: 'middle',
      width: faceSize,
    };

    const selectedDivStyle = Object.assign({}, divStyle, {
      border: selectedFaceBorderWidth + 'px solid rgb(57,126,230)',
      height: faceSize - (2 * selectedFaceBorderWidth),
      width: faceSize - (2 * selectedFaceBorderWidth),
    });

    return allFaces.map(faceState => {
      const {face, url} = faceState;
      const size = faceState.size;
      const isSelected = selectedFaces.has(face.name);
      const fw = size.width * face.w;
      const fh = size.height * face.h;
      const sw = faceSize / fw;
      const sh = faceSize / fh;
      const s = Math.abs(sw) <= Math.abs(sh) ? sw : sh;
      const nw = size.width * s;
      const nh = size.height * s;
      let nx = face.x * nw;
      let ny = face.y * nh;
      if (isSelected) {
        nx += selectedFaceBorderWidth;
        ny += selectedFaceBorderWidth;
      }
      const onClick = () => {
        this._toggleSelectedFace(!isSelected, face.name);
      };
      return <div key={face.name}
                  style={isSelected ? selectedDivStyle : divStyle}
                  onClick={onClick}>
        <img src={url} style={{
          position: 'absolute',
          left: -nx,
          top: -ny,
          width: nw,
          height: nh,
        }}/>
      </div>;
    });
  }

  _toggleSelectedFace(on: boolean, face: string) {
    const {selectedDate, selectedFaces, selectedTags} = this.state;
    this._setFilters(selectedDate, withOrWithout(selectedFaces, on, face), selectedTags);
  }

  _toggleSelectedTag(on: boolean, tag: string) {
    const {selectedDate, selectedFaces, selectedTags} = this.state;
    this._setFilters(selectedDate, selectedFaces, withOrWithout(selectedTags, on, tag));
  }

  _setFilters(date: Date, faces: Set<string>, tags: Set<string>) {
    const settingFilters =
      this._settingFilters.then(() => {
        if (this._settingFilters !== settingFilters) {
          // Since creating this promise, the filters have been set to something more recent.
          // Break now to save the effort of doing a bunch of fetches.
          // TODO: The problem with this is that it will result in false positives in devtools when
          // "break on exception" is turned on.
          throw new BreakException();
        }
      })
      .then(() => this._createPhotosIter(date, faces, tags))
      .then(iter => {
        if (this._settingFilters !== settingFilters) {
          // Since creating this promise, the filters have been set to something more recent.
          // Don't set them in case it will override the previous state.
          return;
        }
        if (this._isMounted) {
          this.setState({
            photosIter: iter,
            selectedDate: date,
            selectedFaces: faces,
            selectedTags: tags,
          });
        }
      }).catch(e => {
        if (!(e instanceof BreakException)) {
          throw e;
        }
      });
    this._settingFilters = settingFilters;
  }

  async _fetch(): Promise<void> {
    const {index} = this.props;
    const [minMaxDates, allFaces, allTags] = await Promise.all([
      this._fetchMinMaxDates(),
      this._fetchFaces(),
      this._fetchTags(),
    ]);
    if (this._isMounted) {
      this.setState({
        allFaces,
        allTags,
        minDate: minMaxDates[0],
        maxDate: minMaxDates[1],
        photosIter: SinglePhotoSetIterator.all(index.byDate),
      });
    }
  }

  _fetchMinMaxDates(): Promise<[Date, Date]> {
    const {index} = this.props;
    const now = new Date();
    const dateFromEntry = entry => new Date(-entry[0] / nanosInMillis);
    return Promise.all([index.byDate.last().then(last => last ? dateFromEntry(last) : now),
                        index.byDate.first().then(fst => fst ? dateFromEntry(fst) : now)]);
  }

  _fetchTags(): Promise<string[]> {
    const {index} = this.props;
    return this._getKeysByCount(index.tagsByCount, 20);
  }

  async _createPhotosIter(selectedDate: Date, selectedFaces: Set<string>, selectedTags: Set<string>)
      : Promise<PhotoSetIterator> {
    const selectedKey = -selectedDate.getTime() * nanosInMillis;
    const {index} = this.props;
    if (selectedTags.size === 0 && selectedFaces.size === 0) {
      return SinglePhotoSetIterator.at(index.byDate, selectedKey, null);
    }

    const photoSetPs = [];
    for (const tag of selectedTags) {
      photoSetPs.push(index.byTag.get(tag));
    }
    for (const face of selectedFaces) {
      photoSetPs.push(index.byFace.get(face));
    }
    const photoSets = (await Promise.all(photoSetPs)).filter(Boolean);

    switch (photoSets.length) {
      case 0:
        return new EmptyIterator();
      case 1:
        return SinglePhotoSetIterator.at(photoSets[0], selectedKey, null);
      default:
        return new PhotoSetIntersectionIterator(photoSets, selectedKey);
    }
  }

  async _getKeysByCount(m: CountMap, limit: number): Promise<string[]> {
    const result = [];
    const it1 = m.iterator();
    for (let e1 = await it1.next(); !e1.done; e1 = await it1.next()) {
      const it2 = e1.value[1].iterator();
      for (let e2 = await it2.next(); !e2.done; e2 = await it2.next()) {
        result.push(e2.value);
        if (result.length === limit) {
          return result;
        }
      }
    }
    return result;
  }

  async _fetchFaces(): Promise<FaceState[]> {
    const {index} = this.props;
    const mostCommon = await this._getKeysByCount(index.facesByCount, 4);

    // Get the most recent photo corresponding to each.
    const self = this;
    const faceListP = mostCommon.map(async name => {
      const byDate = notNull(await index.byFace.get(name));
      const [, mostRecent] = notNull(await byDate.first());
      const firstPhoto = notNull(await mostRecent.first());
      const face = await self._getFace(firstPhoto.faces, name);
      const [size, url] = await self._getBestSize(firstPhoto.sizes, face);
      return {face, size, url};
    });

    return Promise.all(faceListP);
  }

  _getFace(faces: NomsSet<Face>, name: string): Promise<Face> {
    return new Promise((resolve, reject) => {
      let didResolve = false;
      faces.forEach(face => {
        if (face.name === name) {
          resolve(face);
          didResolve = true;
        }
      }).then(() => {
        if (!didResolve) {
          reject('Could not find face ' + name);
        }
      });
    });
  }

  async _getBestSize(sizes: NomsMap<PhotoSize, string>, face: Face): Promise<[PhotoSize, string]> {
    let best = null;

    await sizes.forEach((url, size) => {
      const w = face.w * size.width;
      const h = face.h * size.height;
      const d = Math.min(Math.abs(w - faceSize), Math.abs(h - faceSize));
      if (best === null || d < best.diff) {
        best = {diff: d, size, url};
      }
    });

    invariant(best);
    return [best.size, best.url];
  }

  _maybeUpdateRange(e: Event) {
    const target = e.target;
    if (!(target instanceof HTMLInputElement)) {
      return;
    }
    const selectedDate = new Date(Number(target.value));
    const {selectedFaces, selectedTags} = this.state;
    // Set the selectedDate now so that the widget updates, since _setFilters will throttle.
    this.setState({selectedDate});
    this._setFilters(selectedDate, selectedFaces, selectedTags);
  }
}

function withOrWithout<T>(inp: Set<T>, add: boolean, val: T): Set<T> {
  const out = new Set(inp);
  if (add) {
    out.add(val);
  } else {
    out.delete(val);
  }
  return out;
}
