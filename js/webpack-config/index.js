'use strict';

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
      warnings: false,
    },
  }));
}

function defaultExclude(p) {
  return /node_modules/.test(p) && !/node_modules\/noms/.test(p);
}

module.exports = function(options) {
  options = options || {};
  return {
    module: {
      loaders: [
        {
          test: /\.js$/,
          exclude: options.exclude || defaultExclude,
          loader: 'babel-loader',
        },
      ],
    },

    plugins,

    devtool: devMode ? '#inline-source-map' : '',
    watch: devMode,
  };
};
