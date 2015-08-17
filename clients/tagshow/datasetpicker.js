'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var noms = require('noms')
var React = require('react');

var DatasetPicker = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    root: React.PropTypes.instanceOf(noms.Ref).isRequired,
    onChange: React.PropTypes.func.isRequired,
    selected: React.PropTypes.instanceOf(Immutable.Map),
  },

  getInitialState: function() {
    return {
      datasets: Immutable.Set(),
    };
  },

  handleSelectChange: function(e) {
    this.props.onChange(
      this.state.datasets.find(
        ds => ds.get('id') == e.target.value));
  },

  render: function() {
    // Get the datasets
    this.props.root.deref().then(
      heads => heads.first().deref()).then(
      commit => commit.get('value').deref()).then(
      dsRefs => Promise.all(dsRefs.map(ref => ref.deref()))).then(
      datasets => {
        this.setState({datasets: Immutable.Set.of(...datasets)});
      });

    return <form>
      Dataset:
      <select value={this.props.selected.get('id')}
          onChange={this.handleSelectChange}>
        <option/>
        { 
          this.state.datasets.map((v, k) => {
            return <option value={v.get('id')}>{v.get('id')}</option>
          }).toArray()
        }
      </select>
    </form>
  },
});

module.exports = React.createFactory(DatasetPicker);
