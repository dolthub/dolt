// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

const stable = '7';
const next = '8';
let useNext = false;

export default {
  /**
   * The noms version currently being used. This will be the current stable version, until
   * useNext() is called.
   */
  current(): string {
    return useNext ? next : stable;
  },

  /**
   * Whether we are currently using the stable noms version.
   */
  isStable(): boolean {
    return !useNext;
  },

  /**
   * Whether we are currently using the next noms version that is under development.
   */
  isNext(): boolean {
    return useNext;
  },

  /**
   * Sets the noms version to either be the next version or not.
   */
  useNext(v: boolean) {
    useNext = v;
  },
};
