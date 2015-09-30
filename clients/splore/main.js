'use strict';

var buchheim = require('./buchheim.js');
var Layout = require('./layout.js');
var Immutable = require('immutable');
var noms = require('noms');
var queryString = require('query-string');
var React = require('react');
var {Ref} = require('noms');

var data = {nodes: {}, links: {}};
var rootRef = null;

window.onload = function() {
  var target = document.getElementById('root');
  var w = window.innerWidth;
  var h = window.innerHeight;

  noms.getRoot().then(ref => {
    rootRef = ref;
    handleChunkLoad(ref, new Ref(ref));
  });
};

window.onresize = render;

function handleChunkLoad(ref, val, fromRef) {
  var counter = 0
  var process = (ref, val, fromId) => {
    if (typeof val == 'undefined') {
      return null;
    }

    // Assign a unique ID to this node.
    // We don't use the noms ref because we only want to represent values as shared in the graph if they are actually in the same chunk.
    var id;
    if (val instanceof Ref) {
      id = val.ref;
    } else {
      id = ref + '/' + counter++;
    }

    // Populate links.
    if (fromId) {
      (data.links[fromId] || (data.links[fromId] = [])).push(id);
    }

    switch (typeof val) {
      case 'bool':
      case 'number':
      case 'string':
        data.nodes[id] = {name: String(val)};
        break;
    }

    if (val instanceof Blob) {
      data.nodes[id] = {name: `Blob (${val.size})`};
    } else if (Immutable.List.isList(val)) {
      data.nodes[id] = {name: `List (${val.size})`};
      val.forEach(c => process(ref, c, id));
    } else if (Immutable.Set.isSet(val)) {
      data.nodes[id] = {name: `Set (${val.size})`};
      val.forEach(c => process(ref, c, id));
    } else if (Immutable.Map.isMap(val)) {
      var structName = val.get('$name');
      if (structName) {
        data.nodes[id] = {name: structName};
      } else {
        data.nodes[id] = {name: `Map (${val.size})`};
      }
      val.keySeq().filter(k => k != '$name')
        .forEach(k => {
          // TODO: handle non-string keys
          var kid = process(ref, k, id);

          // Start map keys open, just makes it easier to use.
          data.nodes[kid].isOpen = true;

          process(ref, val.get(k), kid);
        });
    } else if (val instanceof Ref) {
      data.nodes[id] = {
        canOpen: true,
        name: val.ref.substr(5, 6),
        fullName: val.ref,
      };
    }

    return id;
  };

  process(ref, val, fromRef);
  render();
}

function handleNodeClick(e, id) {
  if (e.altKey) {
    if (data.nodes[id].fullName) {
      window.prompt("Full ref", data.nodes[id].fullName);
    }
    return;
  }

  if (id.indexOf('/') > -1) {
    if (data.links[id] && data.links[id].length > 0) {
      data.nodes[id].isOpen = !Boolean(data.nodes[id].isOpen);
      render();
    }
  } else {
    data.nodes[id].isOpen = !Boolean(data.nodes[id].isOpen);
    if (data.links[id] || !data.nodes[id].isOpen) {
      render();
    } else {
      noms.readValue(id, noms.getChunk)
        .then(chunk => handleChunkLoad(id, chunk, id));
    }
  }
}

function render() {
  var dt = new buchheim.TreeNode(data, rootRef, null, 0, 0, {});
  buchheim.layout(dt);
  React.render(<Layout tree={dt} data={data} onNodeClick={handleNodeClick}/>, document.body);
}
