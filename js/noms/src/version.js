// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export default '7.2';

const envVal = '1';
const details = 'https://github.com/attic-labs/noms#install-noms';

// Do not extract the environment variable name into a constant. Our build pipeline does not replace
// the value if we do that.
if (process.env.NOMS_VERSION_NEXT !== envVal) {
  throw new Error(
    `WARNING: This is an unstable version of Noms. Data created with it won't be supported.
Please see ${details} for getting the latest supported version.
Or add NOMS_VERSION_NEXT=${envVal} to your environment to proceed with this version.
`);
}
