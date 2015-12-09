// @flow

import React from 'react'; //eslint-disable-line no-unused-lets
import {readValue, HttpStore, Ref, Struct} from 'noms';

const IMAGE_WIDTH_PX = 286;
const IMAGE_HEIGHT_PX = 324;
const BASE_PX = 72;
const BASE_FEET = 1 + 5 / 12;

const ORIGIN_X_PIXELS = IMAGE_WIDTH_PX / 2;
const ORIGIN_Z_PIXELS = IMAGE_HEIGHT_PX - 41;

function feetToPixels(f: number): number {
  // TODO: Find more accurate image/dimensions.
  return 0.8 * f * BASE_PX / BASE_FEET;
}

type Props = {
  pitchListRef: Ref,
  httpStore: HttpStore
};

type State = {
  loaded: boolean,
  pitchList: ?Array<Struct>
};

export default class HeatMap extends React.Component<void, Props, State> {
  constructor(props: Props) {
    super(props);

    this.state = {
      loaded: false,
      pitchList: null
    };
  }

  async loadIfNeeded(): Promise<void> {
    if (this.state.loaded) {
      return;
    }

    let pitchList = await readValue(this.props.pitchListRef, this.props.httpStore);
    if (Array.isArray(pitchList)) {
      this.setState({
        loaded: true,
        pitchList: pitchList
      });
    } else {
      throw new Error('Unexpected type of pitchList');
    }
  }

  render(): React.Element {
    this.loadIfNeeded();

    let points = this.getPoints();
    let fillStyle = {
      bottom: 0,
      left: 0,
      position: 'absolute',
      right: 0,
      top: 0
    };
    return <div style={ {
      position: 'relative',
      overflow: 'hidden',
      width: IMAGE_WIDTH_PX,
      height: IMAGE_HEIGHT_PX
    } }>
      <img src="background.jpg" style={fillStyle}/>
      <div style={fillStyle}>
        {points}
      </div>
    </div>;
  }

  getPoints(): Array<any> {
    if (!this.state.loaded) {
      return [];
    }

    if (!this.state.pitchList) {
      throw new Error('pitchList not loaded');
    }

    return this.state.pitchList.map(p => {
      let w = 2;
      let h = 2;
      let x = - w / 2 + ORIGIN_X_PIXELS + feetToPixels(p.get('X'));
      let y = - h / 2 + ORIGIN_Z_PIXELS - feetToPixels(p.get('Z'));
      return <div style={
        {
          position: 'absolute',
          left: x,
          top: y,
          background: 'rgba(0,255,0,0.4)',
          width: w,
          height: h,
          boxShadow: '0px 0px 16px 16px rgba(0,255,0,0.4)',
          borderRadius: '50%'
        }
      }/>;
    });
  }
}
