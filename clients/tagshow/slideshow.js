'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms');
var Photo = require('./photo.js');
var React = require('react');

var containerStyle = {
  position: 'absolute',
  left: 0,
  top: 0,
  width: '100%',
  height: '100%',
  overflow: 'hidden',

  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
};

var imageStyle = {
  width: '100%',
  height: '100%',
  objectFit: 'contain',
};

var SlideShow = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    ds: React.PropTypes.instanceOf(Immutable.Map),
    photos: React.PropTypes.instanceOf(Immutable.Set),
  },

  getInitialState: function() {
    return {
      index: 0,
    }
  },

  handleTimeout: function() {
    var newIndex = this.state.index + 1;
    if (newIndex >= this.props.photos.size) {
      newIndex = 0;
    }
    this.setState({index: newIndex});
  },

  render: function() {
    var photoRef = this.props.photos.get(this.state.index);
    if (!photoRef) {
      return null;
    }

    return (
      <div style={containerStyle}>
        <Item
          key={photoRef.ref}
          photoRef={photoRef}
          onTimeout={this.handleTimeout.bind(this)} />
      </div>
    );
  },
});

var Item = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    onTimeout: React.PropTypes.func.isRequired,
    photoRef: React.PropTypes.instanceOf(noms.Ref),
  },

  getInitialState: function() {
    return {
      timerId: 0,
    };
  },

  componentDidMount: function() {
    this.setState({
      timerId: window.setTimeout(this.props.onTimeout, 3000),
    });
  },

  componentWillUnmount: function() {
    window.clearTimeout(this.state.timerId);
  },

  render: function() {
    return (
      <Photo
        photoRef={this.props.photoRef}
        style={imageStyle}/>
    );
  },
});

module.exports = React.createFactory(SlideShow);
