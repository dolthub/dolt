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
      this.props.photos.map(
        photoRef => <Item key={photoRef.ref} photoRef={photoRef}/>
      ).toArray()
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
      photo: null,
    };
  },

  render: function() {
    this.props.photoRef.deref().then(
      p => this.setState({photo: p}));

    if (this.state.photo == null) {
      return <span>loading...</span>;
    }

    return <img style={imageStyle} src={this.state.photo.get('url')}/>
  },
});

module.exports = React.createFactory(SlideShow);
