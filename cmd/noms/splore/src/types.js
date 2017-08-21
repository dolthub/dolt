// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

export type NodeGraph = {
  keyLinks: {[key: string]: string[]},
  links: {[key: string]: string[]},
  nodes: {[key: string]: SploreNode},
  open: {[key: string]: boolean},
};

// See node and nodeInfo in noms_splore.go
// The types are unioned here for simplicity.
export type SploreNode = {
  children?: SploreNodeChild[], // only in node, not nodeInfo
  hasChildren: boolean,
  id: string,
  name: string,
};

// See nodeChild in noms_splore.go
export type SploreNodeChild = {
  key: SploreNode,
  label: string,
  value: SploreNode,
};
