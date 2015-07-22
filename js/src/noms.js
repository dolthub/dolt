var store = require('./noms_store.js')
var decode = require('./decode.js')

module.exports = {
  getRoot: store.getRoot,
  getDataset: store.getDataset,
  getChunk: store.getChunk,
  readValue: decode.readValue,
  getRef: decode.getRef,
  Ref: decode.Ref
};

