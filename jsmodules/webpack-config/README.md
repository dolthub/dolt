`webpack-config` is the shared webpack config for noms.

To use without changing the default configuration - fine for most purposes - just run webpack like:

```
node_modules/.bin/webpack --config node_modules/@attic/webpack-config/index.js ...
```

`webpack-config` can also be configured (see `configure` in index.js), then required as part of a different webpack config, like:

```
// my.webpack.config.js

module.exports = require('@attic/webpack-config').configure({
  requiredEnvVars: ['MY_ENV_VARIABLE']
});
```
