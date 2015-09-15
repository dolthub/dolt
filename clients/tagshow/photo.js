'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');
var server = 'http://localhost:8001';

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
      sizes: null,
    }
  },

  render: function() {
    this.props.photoRef.deref()
      .then(p => this.setState({photo: p}));

    if (this.state.photo === null) {
      return null;
    }

    var area = function(size) {
      return size.get('width') * size.get('height');
    };

    if (this.state.photo.has('sizes')) {
      var p = this.state.photo;
      this.state.photo.get('sizes').deref()
        .then(sizeMap => {
          var sizes = [];
          sizeMap.forEach((url, sizeRef) => sizes.push({size: sizeRef, url: url}));
          sizes = sizes.map(size => {
            return size.size.deref().then(sizeVal => {
              return sizeVal.set('url', size.url);
            });
          });
          return Promise.all(sizes).then(sizes => {
            this.setState({
              sizes: Immutable.Set(sizes).sort((a, b) => area(a) - area(b)),
            });
          });
        });
    } else {
      this.setState({sizes: Immutable.Set()});
    }

    if (this.state.sizes === null) {
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
    // If there are some remote URLs we can use, just pick the most appropriate size. We need the smallest one that is bigger than our current dimensions.
    if (!this.state.sizes.isEmpty()) {
      return this.state.sizes.find((size, url) => {
        var w = this.props.style.width || 0;
        var h = this.props.style.height || 0;
        return size.get('width') >= w && size.get('height') >= h;
      }, this).get('url');
    }

    // Otherwise assume there must be an image blob.
    var url = server + "/?ref=" +
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
module.exports.setServer = function(val) {
  server = val;
};
