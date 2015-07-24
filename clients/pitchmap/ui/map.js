'use strict';

var React = require('react');

const IMAGE_WIDTH_PX = 286;
const IMAGE_HEIGHT_PX = 324;
const BASE_PX = 72;
const BASE_FEET = 1 + 5 / 12;
const FEETS_TO_PIXELS = BASE_PX / BASE_FEET;

const ORIGIN_X_PIXELS = IMAGE_WIDTH_PX / 2;
const ORIGIN_Z_PIXELS = IMAGE_HEIGHT_PX - 41;

function feetToPixels(f) {
  // TODO: Find more accurate image/dimensions.
  return 0.8 * f * BASE_PX / BASE_FEET;
}

var Map = React.createClass({
  propTypes: {
    points: React.PropTypes.object.isRequired,
  },

  loadIfNeeded() {
    if (this.state.loaded) {
      return;
    }

    this.props.points.deref().then((list) => {
      return Promise.all(list.map(p => p.deref()))
    }).then((points) => {
      this.setState({
        points: points,
        loaded: true
      });
    });
  },

  getInitialState() {
    return {
      loaded: false,
      points: null
    };
  },

  render() {
    this.loadIfNeeded();
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
    if (!this.state.loaded) {
      return [];
    }

    return this.state.points.map((p) => {
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
    });
  },
});

module.exports = Map;
