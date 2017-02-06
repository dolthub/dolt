// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

// Tell flow about these. When running the tests these are globals.
declare var afterAll;
declare var afterEach;
declare var beforeAll;
declare var beforeEach;
declare var describe;
declare var it;
declare var jest;

export {
  afterAll as suiteTeardown,
  afterEach as teardown,
  beforeAll as suiteSetup,
  beforeEach as setup,
  describe as suite,
  it as test,
  jest as default,
};
