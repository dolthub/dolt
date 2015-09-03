'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');

var Photo = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    onTimeout: React.PropTypes.func.isRequired,
    photoRef: React.PropTypes.instanceOf(noms.Ref),
    style: React.PropTypes.object,
  },

  getInitialState: function() {
    return {
      blob: null,
    };
  },

  handleLoad: function(e) {
    URL.revokeObjectURL(e.target.src);
  },

  render: function() {
    this.props.photoRef.deref().then(
      p => p.get('image').deref()).then(
      b => this.setState({blob: b}));

    if (this.state.blob == null) {
      return <span style={this.props.style}>loading...</span>;
    }

    return (
      <img
        style={this.props.style}
        src={URL.createObjectURL(this.state.blob)}
        onLoad={this.handleLoad}/>
    );
  },
});

module.exports = React.createFactory(Photo);
