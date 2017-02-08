// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import ReactDOM from 'react-dom';
import GraphiQL from 'graphiql';

window.onload = render;
const server = 'http://localhost:8000';
const endPoint = '/graphql/';
const ds = 'sfcrime';
const auth = '';

function graphQLFetcher(graphQLParams) {
  const url = `${server}${endPoint}?ds=${ds}&query=${graphQLParams.query}`;

  const headers = new Headers();
  if (auth) {
    headers.append('Authorization', `Bearer ${auth}`);
  }

  const req = new Request(url, {
    headers: headers,
    method: 'GET',
    mode: 'cors',
  });

  console.log(url);
  return fetch(req).then(response => response.json());
}

function render() {
  const graphqlElm =
    <GraphiQL fetcher={graphQLFetcher}>
      <GraphiQL.Toolbar>
        server: <input></input>
        ds: <input></input>
      </GraphiQL.Toolbar>
    </GraphiQL>;
  ReactDOM.render(graphqlElm, document.body);
}

