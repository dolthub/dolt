// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import ReactDOM from 'react-dom';
import GraphiQL from 'graphiql';

window.onload = load;

let params = {};

function load() {
  try {
    loadUnsafe();
  } catch (e) {
    renderPrompt(e.message);
  }
}

function loadUnsafe() {
  params = getParams(location.href);

  if (!params.db) {
    renderPrompt('Noms GraphQL Endpoint?');
    return;
  }

  render();
}

function getParams(href: string): {[key: string]: string} {
  // This way anything after the # will end up in params, which is what we want.
  const paramsIdx = href.indexOf('?');
  if (paramsIdx === -1) {
    return {};
  }

  return decodeURIComponent(href.slice(paramsIdx + 1)).split('&').reduce((params, kv) => {
    // Make sure that '=' characters are preserved in query values, since '=' is valid base64.
    const eqIndex = kv.indexOf('=');
    if (eqIndex === -1) {
      params[kv] = '';
    } else {
      params[kv.slice(0, eqIndex)] = kv.slice(eqIndex + 1);
    }
    return params;
  }, {});
}

type PromptProps = {
  msg: string,
};

class Prompt extends React.Component<void, PromptProps, void> {
  render(): React.Element<any> {
    const fontStyle: {[key: string]: string} = {
      fontFamily: 'Menlo',
      fontSize: '14px',
    };
    const divStyle = {
      alignItems: 'center',
      display: 'flex',
      height: '100%',
      justifyContent: 'center',
    };
    const inputStyle = Object.assign(fontStyle, {}, {
      marginBottom: '0.5em',
      width: '50ex',
    });
    const server = 'http://localhost:8000';

    return <div style={divStyle}>
      <div style={fontStyle}>
        {this.props.msg}
        <form style={{margin:'0.5em 0'}} onSubmit={e => this._handleOnSubmit(e)}>
          <input type='text' ref='db' autoFocus={true} style={inputStyle}
            defaultValue={params.db || server} placeholder={`database (e.g. ${server})`}
          />
          <input type='text' ref='ds' style={inputStyle}
            defaultValue={params.ds} placeholder={'dataset (e.g. sf-film-locations)'}
          />
          <input type='text' ref='auth' style={inputStyle}
            defaultValue={params.auth} placeholder={'auth token'}
          />
          <button type='submit'>OK</button>
        </form>
      </div>
    </div>;
  }

  _handleOnSubmit(e) {
    e.preventDefault();
    const qs = ['db', 'ds', 'auth']
      .map(k => [k, this.refs[k].value])
      .filter(([, v]) => !!v)
      .map(([k, v]) => `${k}=${v}`)
      .join('&');
    window.history.pushState({}, undefined, qs === '' || ('?' + qs));
    load();
  }
}

function graphQLFetcher(graphQLParams) {
  const url = `${params.db}/graphql/`;

  const headers = new Headers();
  headers.append('Content-Type', 'application/x-www-form-urlencoded; charset=utf-8');
  if (params.auth) {
    headers.append('Authorization', `Bearer ${params.auth}`);
  }

  return fetch(url, {
    body: `ds=${params.ds}&query=${encodeURIComponent(graphQLParams.query)}`,
    headers,
    method: 'POST',
    mode: 'cors',
  }).then(response => response.json());
}

function renderPrompt(msg: string) {
  ReactDOM.render(<Prompt msg={msg}/>, document.body);
}

function render() {
  ReactDOM.render(<GraphiQL fetcher={graphQLFetcher}></GraphiQL>, document.body);
}
