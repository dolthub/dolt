// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// @flow

import React from 'react';
import ReactDOM from 'react-dom';
import GraphiQL from 'graphiql';

window.onload = load;
window.onpopstate = load;

let params = {};

function load() {
  try {
    loadUnsafe();
  } catch (e) {
    renderPrompt(e.message);
  }
}

function loadUnsafe() {
  params = getParams(location.search);

  if (!params.db) {
    renderPrompt('Noms GraphQL Endpoint?');
    return;
  }

  render();
}

function getParams(search: string): {[key: string]: string} {
  if (search[0] !== '?') {
    return {};
  }

  const params = {};
  for (const param of search.slice(1).split('&')) {
    const eq = param.indexOf('=');
    params[param.slice(0, eq)] = decodeURIComponent(param.slice(eq + 1));
  }

  return params;
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
    const headerStyle = {
      fontSize: '16px',
      fontWeight: 'bold',
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

    const defaults = {
      db: 'http://localhost:8000',
      ds: 'photoindexer/all/run',
      endpoint: 'graphql',
    };

    return <div style={divStyle}>
      <div style={fontStyle}>
        <span style={headerStyle}>{this.props.msg}</span>
        <form style={{margin:'0.5em 0'}} onSubmit={e => this._handleOnSubmit(e)}>

          <label>
            Server
            <input type='text' ref='db' autoFocus={true} style={inputStyle}
              defaultValue={params.db || defaults.db}/>
          </label>

          <label>
            API endpoint
            <input type='text' ref='endpoint' style={inputStyle}
              defaultValue={params.endpoint || defaults.endpoint}/>
          </label>

          <label>
            Dataset
            <input type='text' ref='ds' style={inputStyle} defaultValue={params.ds || defaults.ds}/>
          </label>

          <label>
            Auth token
            <input type='text' ref='auth' style={inputStyle} defaultValue={params.auth}/>
          </label>

          <label>
            Extra args
            <input type='text' ref='extra' style={inputStyle} defaultValue={params.extra}/>
          </label>

          <button type='submit'>OK</button>
        </form>
      </div>
    </div>;
  }

  _handleOnSubmit(e) {
    e.preventDefault();
    const qs = ['db', 'endpoint', 'ds', 'auth', 'extra']
      .map(k => [k, this.refs[k].value])
      .filter(([, v]) => !!v)
      .map(([k, v]) => `${k}=${encodeURIComponent(v)}`)
      .join('&');
    window.history.pushState({}, undefined, qs === '' || ('?' + qs));
    load();
  }
}

function graphQLFetcher(graphQLParams) {
  const url = `${params.db}/${params.endpoint}`;

  const headers = new Headers();
  headers.append('Content-Type', 'application/x-www-form-urlencoded; charset=utf-8');
  if (params.auth) {
    headers.append('Authorization', `Bearer ${params.auth}`);
  }

  const vars = graphQLParams.variables ?
      `&vars=${encodeURIComponent(JSON.stringify(graphQLParams.variables))}` : '';

  const extra = params.extra ? `&${params.extra}` : '';
  return fetch(url, {
    body: `ds=${params.ds}&query=${encodeURIComponent(graphQLParams.query)}${vars}${extra}`,
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
