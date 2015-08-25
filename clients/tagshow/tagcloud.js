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
    ds: React.PropTypes.instanceOf(Immutable.Map),
    selected: React.PropTypes.instanceOf(Immutable.Set),
    onChoose: React.PropTypes.func.isRequired,
  },

  getInitialState: function() {
    return {
      tags: Immutable.Map(),
    };
  },

  render: function() {
    this.props.ds
      .then(head => head.get('value').deref())
      .then(tags => this.setState({tags: tags}));

    return <div>{
      this.state.tags.keySeq().sort().map(
        tag => {
          var ref = this.state.tags.get(tag);
          return <div style={buttonStyle}>
            <label>
              <input type="checkbox" name="tc"
                checked={this.props.selected.has(tag)}
                onChange={() => this.props.onChoose(tag) }/>
              {tag}
            </label>
          </div>
        }).toArray()
    }</div>
  },
});

module.exports = React.createFactory(TagCloud);
