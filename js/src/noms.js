var store = require('./noms_store.js')
var decode = require('./decode.js')

function getDataset(pRoot, id) {
  return pRoot
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

function getDatasetIds(pRoot) {
  return pRoot
      .then(commit => commit.get('value').deref())
      .then(dsRefs => Promise.all(dsRefs.map(ref => ref.deref())))
      .then(datasets => datasets.map(dataset => dataset.get('id')))
}

module.exports = {
  setServer: store.setServer,
  getRoot: store.getRoot,
  getDataset: getDataset,
  getDatasetIds: getDatasetIds,
  getChunk: store.getChunk,
  readValue: decode.readValue,
  getRef: decode.getRef,
  Ref: decode.Ref
};
