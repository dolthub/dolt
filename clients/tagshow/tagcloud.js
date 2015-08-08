'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var React = require('react');

var buttonStyle = {
  display: 'block',
};

var TagCloud = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    tags: React.PropTypes.instanceOf(Immutable.Map),
    selected: React.PropTypes.instanceOf(Immutable.Set),
    onChoose: React.PropTypes.func.isRequired,
  },

  render: function() {
    return <div>{
      this.props.tags.keySeq().sort().map(
        (tag) => {
          var ref = this.props.tags.get(tag);
          var id = "tc-" + tag;
          return <div style={buttonStyle}>
            <input type="checkbox" name="tc" id={id}
              checked={this.props.selected.has(tag)}
              onChange={() => this.props.onChoose(tag) }/>
            <label htmlFor={id}>{tag}</label>
          </div>
        }).toArray()
    }</div>
  },
});

module.exports = React.createFactory(TagCloud);
