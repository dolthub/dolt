'use strict';

var classNames = require('classnames');
var React = require('react');

var Node = React.createClass({
  propTypes: {
    canOpen: React.PropTypes.bool.isRequired,
    isOpen: React.PropTypes.bool.isRequired,
    shape: React.PropTypes.string.isRequired,
    text: React.PropTypes.string.isRequired,
    title: React.PropTypes.string,
    fromX: React.PropTypes.number.isRequired,
    fromY: React.PropTypes.number.isRequired,
    x: React.PropTypes.number.isRequired,
    y: React.PropTypes.number.isRequired,
    onClick: React.PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      x: this.props.fromX,
      y: this.props.fromY,
    };
  },

  render() {
    if (this.state.x != this.props.x ||
        this.state.y != this.props.y) {
      window.requestAnimationFrame(() => this.setState({
        x: this.props.x,
        y: this.props.y,
      }));
    }

    var textAnchor = 'start';
    var textX = 10;
    var translate = `translate3d(${this.state.x}px, ${this.state.y}px, 0)`;

    if (this.props.canOpen) {
      textAnchor = 'end';
      textX = -10;
    }

    return (
      <g className='node' onClick={this.props.onClick} style={{transform:translate}}>
        {this.getShape()}
        <text x={textX} dy='.35em' textAnchor={textAnchor}>
          {this.props.text}
        </text>
        <title>{this.props.title}</title>
      </g>
    );
  },

  getShape() {
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
