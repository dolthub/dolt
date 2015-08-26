'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');

var containerStyle = {
};

var imageStyle = {
  maxHeight: 300,
  marginRight: 7,
};

var SlideShow = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    ds: React.PropTypes.instanceOf(Immutable.Map),
    tags: React.PropTypes.instanceOf(Immutable.Set),
  },

  getInitialState: function() {
    return {
      photos: Immutable.Set(),
    }
  },

  render: function() {
    this.props.ds
      .then(head => head.get('value').deref())
      .then(tags => {
        return Promise.all(
          tags.filter((v, t) => this.props.tags.has(t))
            .valueSeq()
            .map(ref => ref.deref()))
      }).then(sets => {
        this.setState({photos: Immutable.Set().union(...sets)})
      });

    return <div style={containerStyle}>{
      this.state.photos
        .sort(
          // This sorts the photos deterministically, by the ref of their image
          // blob.
          // TODO: Sort by create date if it ends up that the common image type
          // has a create date.
          (a, b) => a.ref < b.ref)
        .map(
          photoRef => <Item key={photoRef.ref} photoRef={photoRef}/>)
        .toArray()
    }</div>
  },
});

var Item = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    photoRef: React.PropTypes.instanceOf(noms.Ref),
  },

  getInitialState: function() {
    return {
      blob: null,
      tags: Immutable.Set(),
    };
  },

  render: function() {
    this.props.photoRef.deref().then(
      p => p.get('image').deref()).then(
      b => this.setState({blob: b}));

    this.props.photoRef.deref().then(
      p => p.get('tags').deref()).then(
      tags => this.setState({tags: tags}));

    if (this.state.blob == null) {
      return <span>loading...</span>;
    }

    return (
      <div style={{display:'inline-block'}}>
        <img style={imageStyle} src={URL.createObjectURL(this.state.blob)}/>
        <br/>
        {this.state.tags.toArray().join(', ')}
      </div>
    );
  },
});

module.exports = React.createFactory(SlideShow);
