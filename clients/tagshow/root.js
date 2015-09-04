'use strict';

var DataSetPicker = require('./datasetpicker.js');
var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');
var SlideShow = require('./slideshow.js');
var TagChooser = require('./tagchooser.js');

var Root = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    pRoot: React.PropTypes.instanceOf(Promise),
    qs: React.PropTypes.instanceOf(Immutable.Map),
    updateQuery: React.PropTypes.func.isRequired,
  },

  getInitialState: function() {
    return {
      selected: Immutable.Set(),
      selectedPhotos: Immutable.List(),
    };
  },

  handleDataSetPicked: function(ds) {
    this.props.updateQuery(this.props.qs.set('ds', ds));
  },

  getSelectedTags: function() {
    var tags = this.props.qs.get('tags');
    if (tags) {
      tags = tags.split(',');
    } else {
      tags = [];
    }
    return Immutable.Set(tags);
  },

  setSelectedPhotos: function(ds, selectedTags) {
    ds
      .then(head => head.get('value').deref())
      .then(tags => {
        return Promise.all(
          tags.filter((v, t) => selectedTags.has(t))
            .valueSeq()
            .map(ref => ref.deref()))
      }).then(sets => {
        this.setState({
          selectedPhotos:
            Immutable.List(
              Immutable.Set(sets[0])
                  .intersect(...sets)
                  // This sorts the photos deterministically, by the ref of their image
                  // blob.
                  // TODO: Sort by create date if it ends up that the common image type
                  // has a create date.
                  .sort((a, b) => a.ref < b.ref)
            )
        });
      });
  },

  handleTagsChange: function(tags) {
    this.props.updateQuery(this.props.qs.set('tags', tags.toArray().join(',')));
  },

  handleTagsConfirm: function() {
    this.props.updateQuery(this.props.qs.set('show', 1));
  },

  render: function() {
    if (!this.props.qs.get('ds')) {
      return <DataSetPicker pRoot={this.props.pRoot} onChange={this.handleDataSetPicked}/>
    }

    var dataset = noms.getDataset(this.props.pRoot, this.props.qs.get('ds'))
      .then(ref => ref.deref());
    var selectedTags = this.getSelectedTags();

    this.setSelectedPhotos(dataset, selectedTags);

    if (!this.props.qs.get('show') || selectedTags.isEmpty()) {
      return (
        <TagChooser
          ds={dataset}
          selectedPhotos={this.state.selectedPhotos}
          selectedTags={this.getSelectedTags()}
          onChange={this.handleTagsChange}
          onConfirm={this.handleTagsConfirm}/>
      );
    }

    return (
      <SlideShow ds={dataset} photos={this.state.selectedPhotos}/>
    );
  },
});

module.exports = React.createFactory(Root);
