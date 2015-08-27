'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');

var DatasetPicker = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    root: React.PropTypes.instanceOf(Promise),
    onChange: React.PropTypes.func.isRequired,
    selected: React.PropTypes.string,
  },

  getInitialState: function() {
    return {
      datasets: Immutable.Set(),
    };
  },

  handleSelectChange: function(e) {
    this.props.onChange(e.target.value);
  },

  render: function() {
    noms.getDatasetIds(this.props.root).then(
      datasets => {
        this.setState({
          datasets: Immutable.Set.of(...datasets)
        });
      }
    );

    return <form>
      Choose dataset:
      <br/>
      <select value={this.props.selected}
          onChange={this.handleSelectChange}>
        <option/>
        { 
          this.state.datasets.map(v => {
            return <option value={v}>{v}</option>
          }).toArray()
        }
      </select>
    </form>
  },
});

module.exports = React.createFactory(DatasetPicker);
