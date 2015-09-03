'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
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
    tags: React.PropTypes.instanceOf(Immutable.Set),
  },

  getInitialState: function() {
    return {
      photos: Immutable.Set(),
      index: 0,
      nextSlideTime: 0,
    }
  },

  handleTimeout: function() {
    var newIndex = this.state.index + 1;
    if (newIndex >= this.state.photos.size) {
      newIndex = 0;
    }
    this.setState({index: newIndex});
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
        this.setState({
          photos:
            Immutable.List(
              Immutable.Set(sets[0])
                  .intersect(...sets)
                  // This sorts the photos deterministically, by the ref of their image
                  // blob.
                  // TODO: Sort by create date if it ends up that the common image type
                  // has a create date.
                  .sort((a, b) => a.ref < b.ref)
            )
        });
      });

    var photoRef = this.state.photos.get(this.state.index);
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
      blob: null,
      tags: Immutable.Set(),
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

  handleLoad: function(e) {
    URL.revokeObjectURL(e.target.src);
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
      <img
        style={imageStyle}
        src={URL.createObjectURL(this.state.blob)}
        onLoad={this.handleLoad}/>
    );
  },
});

module.exports = React.createFactory(SlideShow);
