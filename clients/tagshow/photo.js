'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');

var Photo = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    onLoad: React.PropTypes.func,
    photoRef: React.PropTypes.instanceOf(noms.Ref),
    style: React.PropTypes.object,
  },

  getInitialState: function() {
    return {
      photo: null,
    }
  },

  render: function() {
    this.props.photoRef.deref()
      .then(p => this.setState({photo: p}));

    if (this.state.photo === null) {
      return null;
    }

    return (
      <img
        style={this.props.style}
        src={this.getURL()}
        onLoad={this.props.onLoad}/>
    );
  },

  getURL: function() {
    var url = "http://localhost:8001/?ref=" +
        this.state.photo.get('image').ref;

    if (this.props.style.width) {
      url += "&maxw=" + this.props.style.width;
    }
    if (this.props.style.height) {
      url += "&maxh=" + this.props.style.height;
    }

    return url;
  }
});

module.exports = React.createFactory(Photo);
