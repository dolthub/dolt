'use strict';

var classNames = require('classnames');
var sprintf = require('sprintf-js').sprintf;
var React = require('react');

var Node = React.createClass({
  propTypes: {
    canOpen: React.PropTypes.bool.isRequired,
    isOpen: React.PropTypes.bool.isRequired,
    shape: React.PropTypes.string.isRequired,
    text: React.PropTypes.string.isRequired,
    fromX: React.PropTypes.number.isRequired,
    fromY: React.PropTypes.number.isRequired,
    x: React.PropTypes.number.isRequired,
    y: React.PropTypes.number.isRequired,
    onToggle: React.PropTypes.func.isRequired,
  },

  getInitialState: function() {
    return {
      x: this.props.fromX,
      y: this.props.fromY,
    };
  },

  render: function() {
    if (this.state.x != this.props.x ||
        this.state.y != this.props.y) {
      window.requestAnimationFrame(() => this.setState({
        x: this.props.x,
        y: this.props.y,
      }));
    }

    var textAnchor = 'start';
    var textX = 10;
    var translate = sprintf(
      'translate3d(%fpx,%fpx,0)',
      this.state.x,
      this.state.y);

    if (this.props.canOpen) {
      textAnchor = 'end';
      textX = -10;
    }

    return (
      <g className='node' onClick={this.props.onToggle} style={{transform:translate}}>
        {this.getShape()}
        <text x={textX} dy='.35em' textAnchor={textAnchor}>
          {this.props.text}
        </text>
      </g>
    );
  },

  getShape: function() {
    var className = classNames('icon', {open:this.props.isOpen});
    switch (this.props.shape) {
      case 'circle':
        return <circle className={className} r='4.5'/>;
      case 'rect':
        // rx:1.35 and ry:1.35 for rounded corners, but not doing until I learn how to make the triangle match below.
        return <rect className={className} x='-4.5' y='-4.5' width='9' height='9'/>;
      case 'triangle':
        return <polygon className={className} points='0,-4.5 4.5,4.5 -4.5,4.5' rx='1.35' ry='1.35'/>
    }
  },
});

module.exports = React.createFactory(Node);
