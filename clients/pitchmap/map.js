var React = require('react');

var Map = React.createClass({
  propTypes: {
    points: React.PropTypes.object.isRequired,
    width: React.PropTypes.number.isRequired,
    height: React.PropTypes.number.isRequired,
  },

  render: function() {
    var points = this.getPoints();
    return React.DOM.div(
      {
        style: {
          position: 'relative',
        },
      },
      React.DOM.img({
        src: 'background.jpg',
        width: this.props.width,
        height: this.props.height,
      }),
      React.DOM.div(
        {
          style: {
            position: 'absolute',
            left: 0,
            top: 0,
            width: this.props.width,
            height: this.props.height,
          },
        },
        points
      )
    );
  },

  getPoints: function() {
    return this.props.points.map(function(p) {
      var w = 20;
      var h = 20;
      var x = (this.props.width - w) * p.get('x');
      var y = (this.props.height - h) * p.get('y');
      return React.DOM.div({
        style: {
          position: 'absolute',
          left: x,
          top: y,
          background: 'rgba(0,255,0,0.4)',
          width: w,
          height: h,
        }
      })
    }, this).toArray();
  },
});

module.exports = Map;
