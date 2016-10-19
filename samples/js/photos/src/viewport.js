// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import Window from './window.js';

export default class Viewport {
  _wnd: Window;
  _scrollToBottomListeners: Set<Function>;

  constructor(wnd: Window, body: HTMLElement) {
    this._wnd = wnd;
    this._scrollToBottomListeners = new Set();

    wnd.addEventListener('scroll', () => {
      if (body.scrollTop + this.clientHeight >= body.offsetHeight) {
        for (const fn of new Set(this._scrollToBottomListeners)) {
          fn();
        }
      }
    });
  }

  get clientHeight(): number {
    return this._wnd.document.documentElement.clientHeight;
  }

  get clientWidth(): number {
    return this._wnd.document.documentElement.clientWidth;
  }

  addScrollToBottomListener(fn: Function) {
    this._scrollToBottomListeners.add(fn);
  }

  removeScrollToBottomListener(fn: Function) {
    this._scrollToBottomListeners.delete(fn);
  }
}
