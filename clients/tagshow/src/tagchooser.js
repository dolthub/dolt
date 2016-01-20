// @flow

import Preview from './preview.js';
import React from 'react';
import TagList from './taglist.js';
import type {ChunkStore, Ref} from 'noms';

const styles = {
  root: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
  },

  panes: {
    display: 'flex',
    flex: 1,
  },

  left: {
    overflowX: 'hidden',
    overflowY: 'auto',
    marginRight: '1em',
  },

  right: {
    flex: 1,
    overflowX: 'hidden',
    overflowY: 'auto',
    padding: '1em',
  },

  bottom: {
    textAlign: 'center',
  },

  button: {
    fontSize: '1.5em',
    margin: '1em',
    width: '50%',
  },
};

type Props = {
  store: ChunkStore,
  tags: Array<string>,
  selectedPhotos: Array<Ref>,
  selectedTags: Set<string>,
  onChange: (selected: Set<string>) => void,
  onConfirm: () => void,
};

export default function TagChooser(props: Props) : React.Element {
  return (
    <div style={styles.root}>
      <div style={styles.panes}>
        <div style={styles.left}>
          <TagList
            tags={props.tags}
            selected={props.selectedTags}
            onChange={props.onChange}/>
        </div>
        <div style={styles.right}>
          <Preview photos={props.selectedPhotos} store={props.store}/>
        </div>
      </div>
      <div style={styles.bottom}>
        <button style={styles.button} onClick={props.onConfirm}>
          PUSH THIS BUTTON
        </button>
      </div>
    </div>
  );
}
