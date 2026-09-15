// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const suites = require('./suites.json');
const DEFAULT_TYPES = ['opened', 'synchronize', 'reopened'];

// The registry uses literal paths and trailing /** only. Reject other glob syntax
// so adding a new filter cannot silently select the wrong suites.
function matches(pattern, value) {
  const recursive = pattern.endsWith('/**');
  const literal = recursive ? pattern.slice(0, -3) : pattern;
  if (/[!*?\[\]{}+]/.test(literal)) throw new Error(`Unsupported CI filter: ${pattern}`);
  return recursive ? value.startsWith(`${literal}/`) : value === literal;
}

function selectSuites({ action, branch, files, truncated = false }) {
  const selected = {};
  for (const [id, suite] of Object.entries(suites)) {
    selected[id] = (suite.types || DEFAULT_TYPES).includes(action) &&
      (!suite.branches || suite.branches.some(pattern => matches(pattern, branch))) &&
      (!suite.paths || truncated || files.some(file => suite.paths.some(pattern => matches(pattern, file))));
  }
  return selected;
}

async function selection({ github, context }) {
  const number = context.payload.pull_request.number;
  const files = await github.paginate(github.rest.pulls.listFiles,
    { ...context.repo, pull_number: number, per_page: 100 });
  // The API returns at most 3,000 files. Conservatively run all path-matching
  // candidates at that limit instead of omitting tests for unreturned files.
  const selected = selectSuites({ action: context.payload.action,
    branch: context.payload.pull_request.base.ref,
    files: files.flatMap(file => [file.filename, file.previous_filename].filter(Boolean)),
    truncated: files.length >= 3000 });
  const { data: current } = await github.rest.pulls.get({ ...context.repo, pull_number: number });
  if (current.state !== 'open' || current.head.sha !== context.payload.pull_request.head.sha) {
    throw new Error('PR head changed while selecting CI suites; use the new commit’s workflow');
  }
  return selected;
}

module.exports = { matches, selectSuites, selection };
