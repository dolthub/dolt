var store = require('./noms_store.js')
var decode = require('./decode.js')

function getDataset(id) {
  return store.getRoot()
      .then(rootRef => decode.readValue(rootRef, store.getChunk))
      .then(root => root.deref())
      .then(commit => commit.get('value').deref())
      .then(dsRefs => Promise.all(dsRefs.map(ref => ref.deref())))
      .then(datasets => {
        var match = datasets.filter(dataset => dataset.get('id') === id);
        if (match.length > 1) {
          throw Error("Um...this can't be good: More than one dataset with id " + id);
        }

        return match.length === 1 ? match[0].get('head') : null
      });
}

function getDatasetIds() {
  return store.getRoot()
      .then(rootRef => decode.readValue(rootRef, store.getChunk))
      .then(root => root.deref())
      .then(commit => commit.get('value').deref())
      .then(dsRefs => Promise.all(dsRefs.map(ref => ref.deref())))
      .then(datasets => datasets.map(dataset => dataset.get('id')))
}

module.exports = {
  getRoot: store.getRoot,
  getDataset: getDataset,
  getDatasetIds: getDatasetIds,
  getChunk: store.getChunk,
  readValue: decode.readValue,
  getRef: decode.getRef,
  Ref: decode.Ref
};
