// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export default '7.1';

const envName = 'NOMS_VERSION_NEXT';
const envVal = '1';
const details = 'https://github.com/attic-labs/noms#install-noms';

if (process.env[envName] !== envVal) {
  throw new Error(
    `WARNING: This is an unstable version of Noms. Data created with it won't be supported.\n` +
    `Please see ${details} for getting the latest supported version.\n` +
    `Or add ${envName}=${envVal} to your environment to proceed with this version.\n`);
}
