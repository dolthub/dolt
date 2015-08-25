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
    rootValue: React.PropTypes.instanceOf(Promise),
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

  handleTagChoose: function(tag) {
    var tags = this.getSelectedTags();
    tags = tags.has(tag) ? tags.delete(tag) : tags.add(tag);
    this.props.updateQuery(this.props.qs.set('tags', tags.toArray().join(',')));
  },

  render: function() {
    // TODO: This is making assumptions about what is inlined and not inlined
    // that are unwarranted. Sadly, until we have search, or more magical
    // transformations, or async functions, or something, everything has to be
    // defensively deref'd.

    if (!this.props.qs.get('ds')) {
      return <div>
        <b>Error: </b> 'ds' hash parameter not found
      </div>
    }

    var dataset = noms.getDataset(this.props.qs.get('ds'))
      .then(ref => ref.deref());

    return (
      <div style={containerStyle}>
        <div>
          <TagCloud ds={dataset} selected={this.getSelectedTags()}
            onChoose={this.handleTagChoose}/>
        </div>
        <div style={slideShowStyle}>
          <SlideShow ds={dataset} tags={this.getSelectedTags()}/>
        </div>
      </div>
    );
  },
});

module.exports = React.createFactory(Root);
