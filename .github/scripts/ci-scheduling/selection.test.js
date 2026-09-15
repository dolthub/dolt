// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const YAML = require('yaml');
const { matches, selectSuites, selection } = require('./selection');
const suites = require('./suites.json');

const select = (options = {}) => selectSuites({ action: 'opened', branch: 'main', files: ['go/a.go'], ...options });
const enabled = result => Object.keys(result).filter(key => result[key]).sort();
const workflow = name => YAML.parse(readFileSync(join(__dirname, '../../workflows', name), 'utf8'));

test('Go changes select existing suites, while workflow and integration paths stay specific', () => {
  const go = select();
  assert.equal(enabled(go).length, 21);
  assert.equal(go['ci-scheduling-tests'], false);
  const integration = select({ files: ['integration-tests/bats/foo.bats'] });
  assert.equal(integration['ci-bats-unix'], true);
  assert.equal(integration['ci-go-tests'], false);
  assert.equal(integration['ci-sql-server-integration-tests'], false);
  assert.equal(select({ files: ['integration-tests/go-sql-server-driver/main.go'] })['ci-sql-server-integration-tests'], true);
  const tooling = select({ files: ['.github/actions/build-dolt/action.yaml'] });
  assert.equal(tooling['ci-bats-unix'], true);
  assert.equal(tooling['ci-scheduling-tests'], true);
  assert.equal(tooling['ci-go-tests'], false);
  assert.equal(select({ files: ['.github/workflows/ci-go-tests.yaml'] })['ci-go-tests'], true);
});

test('documentation-only changes preserve suites without path filters', () => {
  assert.deepEqual(enabled(select({ files: ['README.md'] })), [
    'ci-check-repo', 'doltgres-dependency', 'import-benchmarks-pull-report',
    'merge-perf-pr', 'performance-benchmarks-pull-report', 'sysbench-benchmarks-pull-report',
  ]);
});

test('branch restrictions and opened-only benchmarks preserve their original events', () => {
  const other = select({ branch: 'release' });
  assert.equal(other['ci-go-tests'], false);
  assert.equal(other['ci-orm-tests'], true);
  assert.equal(other['doltgres-dependency'], true);
  assert.equal(select({ action: 'synchronize' })['import-benchmarks-pull-report'], false);
  assert.equal(select({ action: 'reopened' })['ci-go-tests'], true);
  assert.equal(select({ action: 'reopened' })['ci-check-correctness'], false);
  for (const action of ['labeled', 'unlabeled']) {
    assert.deepEqual(enabled(select({ action })), ['ci-check-correctness', 'ci-check-performance']);
  }
});

test('all registered patterns are supported and recursive paths do not match sibling directories', () => {
  for (const suite of Object.values(suites)) {
    for (const pattern of [...suite.paths || [], ...suite.branches || []]) {
      assert.doesNotThrow(() => matches(pattern, 'some/path'));
    }
  }
  assert.equal(matches('go/**', 'go/deep/path/a.go'), true);
  assert.equal(matches('go/**', 'gopher/a.go'), false);
  assert.equal(matches('main', 'main-next'), false);
  assert.throws(() => matches('go/*.go', 'go/a.go'), /Unsupported CI filter/);
});

function fixture(files, current = { state: 'open', head: { sha: 'head' } }) {
  const context = { repo: { owner: 'owner', repo: 'repo' }, payload: {
    action: 'synchronize', pull_request: { number: 7, head: { sha: 'head' }, base: { ref: 'main' } },
  } };
  const github = {
    rest: { pulls: { listFiles: 'listFiles', get: async () => ({ data: current }) } },
    paginate: async (method, args) => {
      assert.equal(method, 'listFiles');
      assert.equal(args.pull_number, 7);
      assert.equal(args.per_page, 100);
      return files;
    },
  };
  return { context, github };
}

test('renames select suites for both old and new paths', async () => {
  const f = fixture([{ filename: 'docs/example.txt', previous_filename: 'go/example.go' }]);
  assert.equal((await selection(f))['ci-go-tests'], true);
});

test('the API file cap conservatively selects suites but retains branch and event restrictions', async () => {
  const f = fixture(Array.from({ length: 3000 }, (_, i) => ({ filename: `docs/${i}.md` })));
  const result = await selection(f);
  assert.equal(result['ci-go-tests'], true);
  assert.equal(result['ci-scheduling-tests'], true);
  assert.equal(result['import-benchmarks-pull-report'], false);
  f.context.payload.pull_request.base.ref = 'release';
  assert.equal((await selection(f))['ci-go-tests'], false);
});

test('a new head or API failure cannot authorize tests with stale file selection', async () => {
  await assert.rejects(selection(fixture([], { state: 'open', head: { sha: 'new' } })), /PR head changed/);
  await assert.rejects(selection(fixture([], { state: 'closed', head: { sha: 'head' } })), /PR head changed/);
  const f = fixture([]);
  f.github.paginate = async () => { throw new Error('API unavailable'); };
  await assert.rejects(selection(f), /API unavailable/);
});

test('one shared gate protects suites and label-only updates cannot replace test checks', () => {
  const parents = ['ci-pr.yaml', 'ci-pr-labels.yaml'].map(workflow);
  assert.deepEqual(parents[0].on.pull_request.types, ['opened', 'synchronize', 'reopened']);
  assert.deepEqual(parents[1].on.pull_request, {
    branches: ['main'], paths: ['go/**'], types: ['opened', 'synchronize', 'labeled', 'unlabeled'],
  });
  assert.deepEqual(Object.keys(parents[1].jobs).sort(), ['ci-admission', 'ci-check-correctness', 'ci-check-performance']);
  const calls = {};
  for (const parent of parents) {
    assert.equal(parent.concurrency, undefined, 'a label event must not cancel the test workflow');
    assert.equal(parent.jobs['ci-admission'].uses, './.github/workflows/ci-admission.yaml');
    assert.deepEqual(parent.jobs['ci-admission'].permissions, { contents: 'read', 'pull-requests': 'read' });
    for (const [id, job] of Object.entries(parent.jobs)) {
      if (id === 'ci-admission') continue;
      assert.equal(calls[id], undefined, 'each suite belongs to exactly one parent');
      calls[id] = job;
      assert.equal(job.needs, 'ci-admission');
      assert.equal(job.if, `needs.ci-admission.outputs.run == 'true' && fromJSON(needs.ci-admission.outputs.suites)['${id}']`);
      assert.equal(job.uses, `./${suites[id].workflow}`);
      assert.equal(job.secrets, 'inherit');
      const child = workflow(suites[id].workflow.split('/').pop());
      assert.ok('workflow_call' in child.on);
      assert.equal(child.on.pull_request, undefined);
      assert.equal(child.jobs['ci-admission'], undefined);
      assert.doesNotMatch(JSON.stringify(child), /needs\.ci-admission/);
      // Explicit permissions in a child cannot exceed the caller's grants.
      for (const childJob of Object.values(child.jobs)) {
        for (const [permission, access] of Object.entries(childJob.permissions || {})) {
          assert.equal(job.permissions[permission], access);
        }
      }
    }
  }
  assert.deepEqual(Object.keys(calls).sort(), Object.keys(suites).sort());
});

test('standalone manual and comment-command suite triggers remain available', () => {
  for (const name of ['ci-bats-unix.yaml', 'ci-binlog-tests.yaml', 'ci-go-tests.yaml', 'ci-orm-tests.yaml', 'ci-scheduling-tests.yaml']) {
    assert.ok('workflow_dispatch' in workflow(name).on, name);
  }
  assert.deepEqual(workflow('ci-orm-tests.yaml').on.repository_dispatch, { types: ['test-orm-integrations'] });
  assert.deepEqual(workflow('doltgres-dependency.yml').on.issue_comment, { types: ['created', 'edited', 'deleted'] });
  for (const name of ['import-benchmarks-pull-report.yaml', 'merge-perf-pr.yaml', 'performance-benchmarks-pull-report.yaml', 'sysbench-benchmarks-pull-report.yaml']) {
    assert.deepEqual(workflow(name).on.issue_comment, { types: ['created'] });
  }
});
