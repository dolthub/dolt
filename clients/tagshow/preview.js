/* @flow */

'use strict';

import Photo from './photo.js';
import React from 'react';
import type {ChunkStore, Ref} from 'noms';

const photoStyle = {
  display: 'inline-block',
  marginRight: 5,
  height: 300
};

type Props = {
  photos: Array<Ref>,
  store: ChunkStore
};

export default function Preview(props: Props) : React.Element {
  return <div>{
    props.photos.map(r => <Photo key={r.toString()} photoRef={r} store={props.store} style={photoStyle}/>)
  }</div>;
}
