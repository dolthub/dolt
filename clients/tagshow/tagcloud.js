'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var React = require('react');

var tagStyle = {
  display: 'inline',
  marginRight: '1ex',
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
      selected: this.props.selected,
      tags: Immutable.Map(),
    };
  },

  handleChange: function(tag) {
    var selected = this.state.selected;
    selected = selected.has(tag) ? selected.delete(tag) : selected.add(tag);
    this.setState({selected: selected});
  },

  handleSubmit: function(e) {
    this.props.onChoose(this.state.selected);
    e.preventDefault();
  },

  render: function() {
    this.props.ds
      .then(head => head.get('value').deref())
      .then(tags => this.setState({tags: tags}));

    return <form onSubmit={this.handleSubmit}>{
      this.state.tags.keySeq().sort().map(
        tag => {
          var ref = this.state.tags.get(tag);
          return (
            <div style={tagStyle}>
              <label>
                <input type="checkbox" name="tc"
                  checked={this.state.selected.has(tag)}
                  onChange={() => this.handleChange(tag) }/>
                {tag}
              </label>
            </div>
          );
        }).toArray()
      }
      <br/>
      <input type="submit" value="OK!"/>
    </form>
  },
});

module.exports = React.createFactory(TagCloud);
