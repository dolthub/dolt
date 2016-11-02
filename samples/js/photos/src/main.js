// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import ReactDOM from 'react-dom';
import {searchToParams} from './params.js';
import Nav from './nav.js';
import PhotosPage from './photos-page.js';
import Viewport from './viewport.js';
import {createPhoto} from './photo.js';
import type {PhotoIndex, NomsPhoto} from './types.js';
import {Path, Spec, Struct} from '@attic/noms';

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
    return <div>Must provide the {notice('?index=')} parameter.</div>;
  }

  let index = indexMap.get(indexStr);
  if (!index) {
    // TODO: Proper auth with localStorage.
    let specOptions;
    if (params.has('access_token')) {
      specOptions = {
        authorization: params.get('access_token'),
      };
    }

    let indexSpec;
    try {
      indexSpec = Spec.forPath(indexStr, specOptions);
    } catch (e) {
      return <div>{notice(indexStr)} is not a valid path: {notice(e.message)}.</div>;
    }

    const indexValue = await indexSpec.value();
    if (!(indexValue instanceof Struct)) {
      return <div>{notice(indexStr)} not found.</div>;
    }

    if (!indexValue.byDate || !indexValue.byFace || !indexValue.byTag ||
        !indexValue.facesByCount || !indexValue.tagsByCount) {
      if (indexStr.endsWith('.value')) {
        return <span>{notice(indexStr)} is not a photo index.</span>;
      }
      return <span>{notice(indexStr)} doesn't look like a photo index, did you leave out&nbsp;
                   {notice('.value')}? <a href={location.href + '.value'}>Try me</a>.</span>;
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

function notice(text: string) {
  return <span style={{
    backgroundColor: 'rgb(238, 206, 206)',
  }}>{text}</span>;
}

main();
