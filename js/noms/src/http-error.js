// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

/**
 * This `Error` class is used to signal an non 2xx response fromg `fetchText` and `fetchUint8Array`.
 */
export default class HttpError extends Error {

  /**
   * The HTTP error code for this error.
   */
  status: number;

  constructor(status: number) {
    super();  // Make Babel happy!

    // Babel does not support extending native classes.
    const e = Object.create(HttpError.prototype);
    e.status = status;
    e.name = 'HttpError';
    e.message = String(status);
    return e;
  }
}
