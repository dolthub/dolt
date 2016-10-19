// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import {invariant} from '@attic/noms';
import Window from './window.js';

export default class Nav {
  _window: Window;
  _listener: () => any;

  constructor(wnd: Window) {
    this._window = wnd;
    this._listener = () => undefined;
    wnd.addEventListener('click', e => this._handleClick(e));
  }

  /**
   * Returns the pathname of the URL that push/replace was called from. For example, if
   * /integrations calls push('/photos') then after navigation, from() will return '/integrations'.
   * If from() is called on an initial page load, from() will return ''.
   */
  from(): string {
    const state = this._window.history.state;
    return state && state.from ? state.from : '';
  }

  back(): void {
    this._window.history.back();
  }

  push(page: string): void {
    const pathname = this._window.location.pathname;
    if (page === pathname) {
      // Only add a history entry if the URL will change.
      return;
    }

    const state = {from: pathname};
    const title = ''; // TODO: get a title from somewhere
    this._window.history.pushState(state, title, page);
    this._listener();
  }

  setListener(f: () => mixed) {
    this._listener = f;
  }

  _handleClick(e: Event) {
    invariant(e instanceof MouseEvent);
    if (e.button !== 0 || e.altKey || e.ctrlKey || e.metaKey || e.shiftKey) {
      // Modifiers to the click event may mean creating a new tab or window. Don't intercept these.
      return;
    }

    // Find target anchor element, which may be nested e.g. <a><img></a>.
    let anchor: ?HTMLAnchorElement;
    invariant(e.target instanceof Element);
    for (let elem = e.target; elem; elem = elem.parentElement) {
      if (elem instanceof HTMLAnchorElement) {
        anchor = elem;
        break;
      }
    }

    if (!anchor || anchor.origin !== this._window.location.origin) {
      // Only intercept non-empty navigations within this origin.
      return;
    }

    e.preventDefault();
    this.push(anchor.pathname + anchor.search);
  }
}
