'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var React = require('react');
var TagList = require('./taglist.js');

var TagChooser = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    ds: React.PropTypes.instanceOf(Immutable.Map),
    selected: React.PropTypes.instanceOf(Immutable.Set),
    onChoose: React.PropTypes.func.isRequired,
  },

  getInitialState: function() {
    return {
      selected: this.props.selected,
      tags: Immutable.Map(),
    };
  },

  handleOnChange: function(selected) {
    this.setState({selected: selected});
  },

  handleSubmit: function(e) {
    this.props.onChoose(this.state.selected);
    e.preventDefault();
  },

  render: function() {
    return (
      <form onSubmit={this.handleSubmit}>
        <input type="submit" value="OK!"/>
        <br/>
        <TagList
          ds={this.props.ds}
          selected={this.state.selected}
          onChange={this.handleOnChange}/>
      </form>
    );
  },
});

module.exports = React.createFactory(TagChooser);
