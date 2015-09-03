'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var React = require('react');

var tagStyle = {
  display: 'block',
};

var TagCloud = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    ds: React.PropTypes.instanceOf(Immutable.Map),
    selected: React.PropTypes.instanceOf(Immutable.Set),
    onChange: React.PropTypes.func.isRequired,
  },

  getInitialState: function() {
    return {
      tags: Immutable.Map(),
    };
  },

  handleChange: function(tag) {
    var selected = this.props.selected;
    selected = selected.has(tag) ? selected.delete(tag) : selected.add(tag);
    this.props.onChange(selected);
  },

  render: function() {
    this.props.ds
      .then(head => head.get('value').deref())
      .then(tags => this.setState({tags: tags}));

    return (
      <div>{
        this.state.tags.keySeq().sort().map(
          tag => {
            var ref = this.state.tags.get(tag);
            return (
              <label style={tagStyle}>
                <input type="checkbox" name="tc"
                  checked={this.props.selected.has(tag)}
                  onChange={() => this.handleChange(tag) }/>
                {tag}
              </label>
            );
          }).toArray()
        }
      </div>
    );
  },
});

module.exports = React.createFactory(TagCloud);
