// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

module.exports = {
  parser: 'babel-eslint',
  rules: {
    'arrow-body-style': ['error', 'as-needed'],
    camelcase: 'error',
    eqeqeq: 'error',
    'flowtype/boolean-style': 'error',
    'flowtype/define-flow-type': 1,
    'flowtype/use-flow-type': 1,
    'no-fallthrough': 'error',
    'no-new-wrappers': 'error',
    'no-throw-literal': 'error',
    'no-unused-vars': ['error', {argsIgnorePattern: '^_$', varsIgnorePattern: '^_$'}],
    'no-var': 'error',
    'prefer-arrow-callback': 'error',
    'prefer-const': 'error',
    'require-yield': 'error',
    radix: 'error',
    'react/jsx-no-duplicate-props': 'error',
    'react/jsx-no-undef': 'error',
    'react/jsx-uses-react': 'error',
    'react/jsx-uses-vars': 'error',
  },
  env: {
    es6: true,
    jest: true,
  },
  extends: 'eslint:recommended',
  ecmaFeatures: {
    jsx: true,
    experimentalObjectRestSpread: true,
  },
  globals: {
    'alert': true,
    'console': true,
    'document': true,
    'fetch': true,
    'window': true,
  },
  plugins: ['flowtype', 'react'],
};
