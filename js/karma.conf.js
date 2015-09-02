module.exports = function(config) {
  config.set({
    frameworks: ['mocha', 'browserify'],

    preprocessors: {
      'test/decode.js': ['browserify']
    },

    files: [
      'test/decode.js'
    ],

    browsers: ['Chrome', 'Firefox', 'IE', 'Safari'],

    client: {
      mocha: {
        reporter: 'html', // change Karma's debug.html to the mocha web reporter
        ui: 'tdd'
      }
    },

    browserify: {
      debug: true,
      transform: ['babelify']
    }
  });
};
