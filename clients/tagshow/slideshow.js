'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');

var imageStyle = {
  display: 'block',
  width: '100%',
};

var SlideShow = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    photos: React.PropTypes.instanceOf(Immutable.Set),
  },

  render: function() {
    return <div>{
      this.props.photos
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
    };
  },

  render: function() {
    this.props.photoRef.deref().then(
      p => p.get('image').deref()).then(
      b => this.setState({blob: b}));

    if (this.state.blob == null) {
      return <span>loading...</span>;
    }

    return <img style={imageStyle} src={URL.createObjectURL(this.state.blob)}/>
  },
});

module.exports = React.createFactory(SlideShow);
