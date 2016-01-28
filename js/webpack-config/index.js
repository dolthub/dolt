'use strict';

const webpack = require('webpack');

const devMode = process.env.NODE_ENV !== 'production';

// Replaces all process.env.foo with the value of the enviroment variable.
function replaceEnv(envVars) {
  const replacements = {
    'process.env.NODE_ENV': JSON.stringify(String(process.env.NODE_ENV)),
  };
  for (const name of envVars) {
    if (!(name in process.env)) {
      console.error(`Missing required environment variable: ${name}`);  //eslint-disable-line
      process.exit(-1);
    }
    replacements[`process.env.${name}`] = JSON.stringify(process.env[name]);
  }
  return new webpack.DefinePlugin(replacements);
}

function getPlugins(envVars) {
  const plugins = [replaceEnv(envVars)];
  if (!devMode) {
    plugins.push(new webpack.optimize.UglifyJsPlugin({
      compress: {
        warnings: false,
        screw_ie8: true,  //eslint-disable-line
      },
    }));
  }
  return plugins;

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

    plugins: getPlugins(options.requiredEnvVars || []),

    devtool: devMode ? '#inline-source-map' : '',
    watch: devMode,
  };
};
