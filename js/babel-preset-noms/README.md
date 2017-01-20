# babel-preset-noms

Babel preset for Noms applications.

## Install

```sh
$ yarn add --dev babel-preset-noms
```

## Usage

### Via `.babelrc` (Recommended)

**.babelrc**

```json
{
  "presets": ["noms"]
}
```

### Via CLI

```sh
$ babel script.js --presets noms
```

### Via Node API

```javascript
require("babel-core").transform("code", {
  presets: ["noms"]
});
```
