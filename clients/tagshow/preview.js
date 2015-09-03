'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var Photo = require('./photo.js');
var React = require('react');

var photoStyle = {
  display: 'inline-block',
  marginRight: '1em',
  maxHeight: 300,
};

var Preview = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    photos: React.PropTypes.instanceOf(Immutable.List),
  },

  render: function() {
    return (
      <div>
        {
          this.props.photos.map(p => <Photo photoRef={p} style={photoStyle}/>).toArray()
        }
      </div>
    );
  },
});

module.exports = React.createFactory(Preview);
