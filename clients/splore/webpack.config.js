'use strict';

const path = require('path');
const webpack = require('webpack');

const devMode = process.env.NODE_ENV !== 'production';

// Replaces all process.env.foo with the value of the enviroment variable.
function replaceEnv() {
  const replacements = {};
  for (const key in process.env) {
    replacements[`process.env.${key}`] = JSON.stringify(process.env[key]);
  }
  return new webpack.DefinePlugin(replacements);
}

const plugins = [replaceEnv()];

if (!devMode) {
  plugins.push(new webpack.optimize.UglifyJsPlugin({
    compress: {
      warnings: false
    }
  }));
}

module.exports = {
  module: {
    loaders: [
      {
        test: /\.js$/,
        exclude: (p) => {
          // Noms needs to be compiled too!
          return /node_modules/.test(p) && !/node_modules\/noms/.test(p)
        },
        loader: 'babel-loader'
      }
    ]
  },

  plugins,

  resolve: {
    alias: {
      // Make sure all of these instances are using the same copy.
      'react': path.resolve('./node_modules/react')
    }
  },

  devtool: devMode ? '#inline-source-map' : '',
  watch: devMode
};
