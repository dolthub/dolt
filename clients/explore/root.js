'use strict';

var React = require('react');
var TreeNode = React.createFactory(require('./tree_node.js'));

var Root = React.createClass({
  propTypes: {
    // I guess it's actually some kind of immutable thingy...?
    rootValue: React.PropTypes.object.isRequired,
  },

  getInitialState: function() {
    return {
      highlightedRef: null,
    };
  },

  handleHighlightRef: function(ref) {
    this.setState({
      highlightedRef: ref,
    });
  },

  render: function() {
    return TreeNode({ 
      name: 'Root',
      value: this.props.rootValue,
      highlightedRef: this.state.highlightedRef,
      onHighlightRef: this.handleHighlightRef,
    });
  },
});

module.exports = React.createFactory(Root);
