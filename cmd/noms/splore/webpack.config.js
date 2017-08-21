// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

'use strict';

const webpack = require('webpack');

const devMode = process.env.NODE_ENV !== 'production';

module.exports = {
  module: {
    loaders: [{
      test: /\.js$/,
      loader: 'babel-loader',
      exclude: /node_modules/,
    }],
  },
  devtool: devMode ? '#inline-source-map' : '',
};
