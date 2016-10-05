// @flow

// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

import React from 'react';
import ReactDOM from 'react-dom';
import {searchToParams} from './params.js';
import Nav from './nav.js';
import PhotosPage from './photos-page.js';
import Viewport from './viewport.js';
import {createPhoto} from './photo.js';
import type {PhotoIndex, NomsPhoto} from './types.js';
import {Path, PathSpec, Struct} from '@attic/noms';

// Cache of index paths to indices. Otherwise calls to render are pretty slow,
// which is noticeable when resizing, toggling between full screen photos, etc.
const indexMap: Map<string, PhotoIndex> = new Map();

function main() {
  const nav = new Nav(window);
  const r = () => {
    const main = document.getElementById('main');
    getRenderElement(nav)
      .then(elem => ReactDOM.render(elem, main))
      .catch(err => console.error(err));
  };
  window.addEventListener('load', r);
  window.addEventListener('resize', r);
  window.addEventListener('popstate', r);
  nav.setListener(r);
}

async function getRenderElement(nav: Nav): Promise<React.Element<any>> {
  const params = searchToParams(location.href);

  const indexStr = params.get('index');
  if (!indexStr) {
    return <div>Must provide an ?index= param.</div>;
  }

  let index = indexMap.get(indexStr);
  if (!index) {
    let indexSpec;
    try {
      indexSpec = PathSpec.parse(indexStr);
    } catch (e) {
      return <div>{indexStr} is not a valid path. {e.message}.</div>;
    }

    const [, indexValue] = await indexSpec.value();
    if (!(indexValue instanceof Struct)) {
      return <div>{indexStr} is not a valid index.</div>;
    }

    // $FlowIssue: can't check instanceof PhotoIndex because it's only a type.
    index = (indexValue: PhotoIndex);
    indexMap.set(indexStr, index);
  }

  let photo = null;
  const photoPath = params.get('photo');
  if (photoPath) {
    // $FlowIssue: can't check instanceof NomsPhoto because it's only a type.
    const nomsPhoto: NomsPhoto = await Path.parse(photoPath).resolve(index);
    photo = await createPhoto(photoPath, nomsPhoto);
  }

  const viewport = new Viewport(window, document.body);

  return <PhotosPage
    index={index}
    nav={nav}
    photo={photo}
    viewport={viewport}
  />;
}

main();
