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

const plugins = [
  plugin('syntax-async-functions'),
  plugin('transform-class-properties'),
  plugin('transform-regenerator'),
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
  plugins
};

const development = {
  presets: [
    preset('es2016'),
    preset('react'),
  ],
  plugins: [
    plugin('transform-es2015-modules-commonjs'),
    ...plugins,
  ],
};

// Env handling is currently broken in Babel.
// https://github.com/babel/babel/issues/4539

const env = process.env.BABEL_ENV || process.env.NODE_ENV;
module.exports = env === 'production' ? production : development;
