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
      <form style={{display:'flex', flexDirection:'column', height:'100%'}} onSubmit={this.handleSubmit}>
        <div style={{display:'flex', flex:1}}>
          <div style={{overflowX:'hidden', overflowY:'auto', marginRight:'1em'}}>
            <TagList
              ds={this.props.ds}
              selected={this.props.selectedTags}
              onChange={this.props.onChange}/>
          </div>
          <div style={{flex:1, overflowX:'hidden', overflowY:'auto', padding:'1em'}}>
            <Preview photos={this.props.selectedPhotos}/>
          </div>
        </div>
        <div style={{textAlign:'center'}}>
          <input style={{fontSize:'2em', fontWeight:'bold', margin:'1em', width:'50%'}} type="submit" value="PUSH BUTTON"/>
        </div>
      </form>
    );
  },
});

module.exports = React.createFactory(TagChooser);
