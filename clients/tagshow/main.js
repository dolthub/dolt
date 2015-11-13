/* @flow */

'use strict';

import {HttpStore} from 'noms';
import queryString from 'query-string';
import React from 'react'; // eslint-disable-line no-unused-vars
import ReactDOM from 'react-dom';
import Root from './root.js';

window.onload = window.onhashchange = render;

function updateQuery(qs: {[key: string]: string}) {
  location.hash = queryString.stringify(qs);
}

function render() {
  let qs = Object.freeze(queryString.parse(location.hash));
  let target = document.getElementById('root');

  let nomsServer;
  if (qs.server) {
    nomsServer = qs.server;
  } else {
    nomsServer = `${location.protocol}//${location.hostname}:8000`;
  }

  let store = new HttpStore(nomsServer);

  ReactDOM.render(
      <Root qs={qs} store={store} updateQuery={updateQuery}/>,
      target);
}
