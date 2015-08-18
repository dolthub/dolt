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
    rootValue: React.PropTypes.instanceOf(noms.Ref),
  },

  getInitialState: function() {
    return {
      selectedDs: Immutable.Map(),
      tags: Immutable.Map(),
      selected: Immutable.Set(),
      selectedPhotos: Immutable.Set(),
    };
  },

  handleDataSetPicked: function(ds) {
    this.setState({selectedDs: ds});

    ds.get('heads').deref().then(
      heads => heads.first().deref()).then(
      commit => commit.get('value').deref()).then(
      tags => this.setState({tags:tags}));
  },

  handleTagChoose: function(tag) {
    this.setState({
      selected: this.state.selected.has(tag) ?
        this.state.selected.remove(tag) :
        this.state.selected.add(tag),
    });
  },

  render: function() {
    // TODO: This is making assumptions about what is inlined and not inlined
    // that are unwarranted. Sadly, until we have search, or more magical
    // transformations, or async functions, or something, everything has to be
    // defensively deref'd.

    // Get the selected photos
    var selectedSetRefs = this.state.tags.filter(
      (v, k) => this.state.selected.has(k)).valueSeq();
    Promise.all(selectedSetRefs.map(r => r.deref())).then(
      sets => {
        this.setState({
          selectedPhotos: Immutable.Set().union(...sets)
        });
      });

    return <div>
      <DataSetPicker
        root={this.props.rootValue}
        selected={this.state.selectedDs}
        onChange={this.handleDataSetPicked}/>
      <br/>
      <div style={containerStyle}>
        <div>
          <TagCloud tags={this.state.tags} selected={this.state.selected}
            onChoose={this.handleTagChoose}/>
        </div>
        <div style={slideShowStyle}>
          <SlideShow photos={this.state.selectedPhotos}/>
        </div>
      </div>
    </div>
  },
});

module.exports = React.createFactory(Root);
