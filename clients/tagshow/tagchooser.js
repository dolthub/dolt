'use strict';

var Immutable = require('immutable');
var ImmutableRenderMixin = require('react-immutable-render-mixin');
var Preview = require('./preview.js');
var React = require('react');
var TagList = require('./taglist.js');

var TagChooser = React.createClass({
  mixins: [ImmutableRenderMixin],

  propTypes: {
    ds: React.PropTypes.instanceOf(Immutable.Map),
    selectedPhotos: React.PropTypes.instanceOf(Immutable.Set),
    selectedTags: React.PropTypes.instanceOf(Immutable.Set),
    onChange: React.PropTypes.func.isRequired,
    onChoose: React.PropTypes.func.isRequired,
  },

  handleSubmit: function(e) {
    this.props.onChoose();
    e.preventDefault();
  },

  render: function() {
    return (
      <table width="100%">
        <tr>
          <td style={{verticalAlign: 'top'}}>
            <form onSubmit={this.handleSubmit}>
              <input type="submit" value="OK!"/>
              <br/>
              <TagList
                ds={this.props.ds}
                selected={this.props.selectedTags}
                onChange={this.props.onChange}/>
            </form>
          </td>
          <td style={{verticalAlign: 'top'}} width="100%">
            <Preview photos={this.props.selectedPhotos}/>
          </td>
        </tr>
      </table>
    );
  },
});

module.exports = React.createFactory(TagChooser);
