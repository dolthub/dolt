// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

/**
 * Loads an image into the browser cache. The returned promise is resolved when the URL has been
 * cached successfully.
 */
export default function preloadImage(src: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.onload = () => resolve();
    img.onerror = (e) => reject(e);
    img.src = src;
  });
}
