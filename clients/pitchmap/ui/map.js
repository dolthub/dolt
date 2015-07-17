'use strict';

var React = require('react');

const IMAGE_WIDTH_PX = 287;
const IMAGE_HEIGHT_PX = 330;
const BASE_PX = 72;
const BASE_FEET = 1 + 5 / 12;
const FEETS_TO_PIXELS = BASE_PX / BASE_FEET;

const ORIGIN_X_PIXELS = IMAGE_WIDTH_PX / 2;
const ORIGIN_Z_PIXELS = IMAGE_HEIGHT_PX - 41;

function feetToPixels(f) {
  return f * BASE_PX / BASE_FEET;
}

var Map = React.createClass({
  propTypes: {
    points: React.PropTypes.object.isRequired,
  },

  render() {
    var points = this.getPoints();
    var fillStyle = {
      bottom: 0,
      left: 0,
      position: 'absolute',
      right: 0,
      top: 0,
    };
    return <div style={ {
      position: 'relative',
      overflow: 'hidden',
      width: IMAGE_WIDTH_PX,
      height: IMAGE_HEIGHT_PX,
    } }>
      <img src="background.jpg" style={fillStyle}/>
      <div style={fillStyle}>
        {points}
      </div>
    </div>;
  },

  getPoints: function() {
    return this.props.points.map(function(p) {
      var w = 2;
      var h = 2;
      var x = - w / 2 + ORIGIN_X_PIXELS + feetToPixels(p.get('X'));
      var y = - h / 2 + ORIGIN_Z_PIXELS - feetToPixels(p.get('Z'));
      return <div style={ {
          position: 'absolute',
          left: x,
          top: y,
          background: 'rgba(0,255,0,0.4)',
          width: w,
          height: h,
          boxShadow: '0px 0px 16px 16px rgba(0,255,0,0.4)',
          borderRadius: '50%',
        } }/>;
    }, this).toArray();
  },
});

module.exports = Map;
