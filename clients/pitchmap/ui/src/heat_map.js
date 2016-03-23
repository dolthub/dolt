// @flow

import React from 'react';
import {DataStore, NomsList, notNull, RefValue} from '@attic/noms';

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
  pitchListRefP: Promise<?RefValue>,
  datastore: DataStore
};

type Point = {
  x: number,
  y: number
};

type State = {
  loaded: boolean,
  pointList: Array<Point>
};

export default class HeatMap extends React.Component<void, Props, State> {
  state: State;

  constructor(props: Props) {
    super(props);

    this.state = {
      loaded: false,
      pointList: [],
    };
  }

  async loadIfNeeded(): Promise<void> {
    if (this.state.loaded) {
      return;
    }

    const pitchListRef = notNull(await this.props.pitchListRefP);
    const pitchList = await this.props.datastore.readValue(pitchListRef.targetRef);

    if (pitchList instanceof NomsList) {
      const pointList = [];
      await pitchList.forEach(p => {
        pointList.push({
          x: -1 + ORIGIN_X_PIXELS + feetToPixels(p.get('X')),
          y: -1 + ORIGIN_Z_PIXELS - feetToPixels(p.get('Z')),
        });
      });
      this.setState({
        loaded: true,
        pointList: pointList,
      });
    } else {
      throw new Error('Unexpected type of pitchList');
    }
  }

  render(): React.Element {
    this.loadIfNeeded();

    const points = this.getPoints();
    const fillStyle = {
      bottom: 0,
      left: 0,
      position: 'absolute',
      right: 0,
      top: 0,
    };
    return (
      <div style={{
        position: 'relative',
        overflow: 'hidden',
        width: IMAGE_WIDTH_PX,
        height: IMAGE_HEIGHT_PX,
      }}>
        <img src="background.jpg" style={fillStyle}/>
        <div style={fillStyle}>
          {points}
        </div>
      </div>
    );
  }

  getPoints(): Array<React.Element> {
    return this.state.pointList.map(p => <div style={{
      position: 'absolute',
      left: p.x,
      top: p.y,
      background: 'rgba(0,255,0,0.4)',
      width: '2px',
      height: '2px',
      boxShadow: '0px 0px 16px 16px rgba(0,255,0,0.4)',
      borderRadius: '50%',
    }}/>);
  }
}
