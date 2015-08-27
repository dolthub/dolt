'use strict';

var DataSetPicker = require('./datasetpicker.js');
var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');
var SlideShow = require('./slideshow.js');
var TagCloud = require('./tagcloud.js');

var containerStyle = {
  display: 'flex',
};

var slideShowStyle = {
  flex: 1,
};

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
      selectedPhotos: Immutable.Set(),
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

  handleTagChoose: function(tags) {
    this.props.updateQuery(this.props.qs.set('tags', tags.toArray().join(',')));
  },

  render: function() {
    if (!this.props.qs.get('ds')) {
      return <DataSetPicker pRoot={this.props.pRoot} onChange={this.handleDataSetPicked}/>
    }

    var dataset = noms.getDataset(this.props.pRoot, this.props.qs.get('ds'))
      .then(ref => ref.deref());

    var selectedTags = this.getSelectedTags();
    if (selectedTags.size === 0) {
      return (
        <TagCloud
          ds={dataset}
          selected={this.getSelectedTags()}
          onChoose={this.handleTagChoose}/>
      );
    }

    return (
      <div style={containerStyle}>
        <div style={slideShowStyle}>
          <SlideShow ds={dataset} tags={selectedTags}/>
        </div>
      </div>
    );
  },
});

module.exports = React.createFactory(Root);
