// @flow

import DatasetPicker from './datasetpicker.js';
import eq from './eq.js';
import React from 'react';
import SlideShow from './slideshow.js';
import TagChooser from './tagchooser.js';
import type {ChunkStore} from 'noms';
import {invariant, NomsMap, NomsSet, readValue, Ref, Struct} from 'noms';

type QueryStringObject = {[key: string]: string};

type Props = {
  store: ChunkStore,
  qs: QueryStringObject,
  updateQuery: (qs: QueryStringObject) => void,
};

type State = {
  selectedTags: Set<string>,
  selectedPhotos: Array<Ref>,
  tags: Array<string>,
};

export default class Root extends React.Component<void, Props, State> {
  constructor(props: Props) {
    super(props);
    this.state = {
      selectedTags: new Set(),
      selectedPhotos: [],
      tags: [],
    };
  }

  async _updateState(props: Props) : Promise<void> {
    const selectedTags = this.getSelectedTags(props);
    const tags = [];
    const selectedPhotos: Array<Ref> = [];

    if (props.qs.ds) {
      const {store} = props;
      const rootRef = await props.store.getRoot();
      const datasets: NomsMap<string, Ref> = await readValue(rootRef, props.store);
      const commitRef = await datasets.get(props.qs.ds);
      invariant(commitRef);
      const commit: Struct = await readValue(commitRef, store);
      const v = commit.get('value');
      if (v instanceof NomsMap) {
        const seenRefs: Set<string> = new Set();

        const sets = [];

        await v.forEach((value, tag) => {
          tags.push(tag);
          if (selectedTags.has(tag) && value instanceof NomsSet) {
            sets.push(value);
          }
        });

        for (const s of sets) {
          await s.forEach(r => {
            const rs = r.toString();
            if (!seenRefs.has(rs)) {
              seenRefs.add(rs);
              selectedPhotos.push(r);
            }
          });
        }

        // This sorts the photos deterministically, by the ref
        // TODO: Sort by create date if it ends up that the common image type
        // has a create date.
        selectedPhotos.sort((a, b) => a.equals(b) ? 0 : a.less(b) ? -1 : 1);
        tags.sort();
      }

      this.setState({selectedTags, tags, selectedPhotos});
    }
  }

  shouldComponentUpdate(nextProps: Props, nextState: State) : boolean {
    return !eq(nextProps, this.props) || !eq(nextState, this.state);
  }

  handleDataSetPicked(ds: string) {
    const qs = Object.assign({}, this.props.qs, {ds});
    this.props.updateQuery(qs);
  }

  getSelectedTags(props: Props) : Set<string> {
    const tags = props.qs.tags;
    if (!tags) {
      return new Set();
    }
    return new Set(tags.split(','));
  }

  handleTagsChange(selectedTags: Set<string>) {
    // FIXME: https://github.com/facebook/flow/issues/1059
    const workaround: any = selectedTags;
    const tags = [...workaround].join(',');
    const qs = Object.assign({}, this.props.qs, {tags});
    this.props.updateQuery(qs);
  }

  handleTagsConfirm() {
    const qs = Object.assign({}, this.props.qs, {show: '1'});
    this.props.updateQuery(qs);
  }

  render() : React.Element {
    this._updateState(this.props);

    if (!this.props.qs.ds) {
      return <DatasetPicker store={this.props.store}
          onChange={ds => this.handleDataSetPicked(ds)}/>;
    }

    if (!this.props.qs.show || this.state.selectedTags.size === 0) {
      return <TagChooser
          store={this.props.store}
          tags={this.state.tags}
          selectedPhotos={this.state.selectedPhotos}
          selectedTags={this.state.selectedTags}
          onChange={selectedTags => this.handleTagsChange(selectedTags)}
          onConfirm={() => this.handleTagsConfirm()}/>;
    }

    return <SlideShow store={this.props.store} photos={this.state.selectedPhotos}/>;
  }
}
