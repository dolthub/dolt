// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

module.exports = require('@attic/eslintrc');
// Allow console
module.exports.rules['no-console'] = 0;
// Used to distinguish between user errors and exceptions.
module.exports.rules['no-throw-literal'] = 0;
