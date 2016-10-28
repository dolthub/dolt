// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

'use strict';

function preset(n) {
  return require('babel-preset-' + n);
}

function plugin(n) {
  return require('babel-plugin-' + n);
}

const commonPlugins = [
  plugin('syntax-async-functions'),
  plugin('syntax-flow'),
  plugin('transform-class-properties'),
  [
    plugin('transform-runtime'), {
      polyfill: false,
      regenerator: true,
    },
  ],
];

const production = {
  presets: [
    preset('es2015'),
    preset('es2016'),
    preset('react'),
  ],
  plugins: [
    ...commonPlugins,
    plugin('transform-regenerator'),
  ],
};

const development = {
  presets: [
    preset('es2016'),
    preset('react'),
  ],
  plugins: [
    ...commonPlugins,
    plugin('transform-es2015-modules-commonjs'),
    plugin('transform-async-to-generator'),
  ],
};

// Env handling is currently broken in Babel.
// https://github.com/babel/babel/issues/4539

const env = process.env.BABEL_ENV || process.env.NODE_ENV;
module.exports = env === 'production' ? production : development;
