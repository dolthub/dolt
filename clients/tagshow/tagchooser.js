'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var Preview = require('./preview.js');
var React = require('react');
var TagList = require('./taglist.js');

var styles = {
  root: {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
  },

  panes: {
    display: 'flex',
    flex: 1,
  },

  left: {
    overflowX: 'hidden',
    overflowY: 'auto',
    marginRight: '1em',
  },

  right: {
    flex: 1,
    overflowX: 'hidden',
    overflowY: 'auto',
    padding: '1em',
  },

  bottom: {
    textAlign: 'center',
  },

  button: {
    fontSize:'1.5em',
    margin:'1em',
    width:'50%',
  },
};

var TagChooser = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    ds: React.PropTypes.instanceOf(Immutable.Map),
    selectedPhotos: React.PropTypes.instanceOf(Immutable.Set),
    selectedTags: React.PropTypes.instanceOf(Immutable.Set),
    onChange: React.PropTypes.func.isRequired,
    onConfirm: React.PropTypes.func.isRequired,
  },

  render: function() {
    return (
      <div style={styles.root}>
        <div style={styles.panes}>
          <div style={styles.left}>
            <TagList
              ds={this.props.ds}
              selected={this.props.selectedTags}
              onChange={this.props.onChange}/>
          </div>
          <div style={styles.right}>
            <Preview photos={this.props.selectedPhotos}/>
          </div>
        </div>
        <div style={styles.bottom}>
          <button style={styles.button} onClick={this.props.onConfirm}>
            PUSH THIS BUTTON
          </button>
        </div>
      </div>
    );
  },
});

module.exports = React.createFactory(TagChooser);
