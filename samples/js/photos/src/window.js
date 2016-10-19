// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

/**
 * Flow doesn't define a Window type. Add properties here as needed.
 */
declare class Window extends EventTarget {
  document: Document;
  history: History;
  location: Location;
}

export default Window;
