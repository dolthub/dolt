// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { ADMISSION_STEP, marker, readDecision, cancelFromJob, cancelDeferredRun } = require('./cancellation');
const deferred = { conclusion: 'cancelled', steps: [{ name: ADMISSION_STEP, conclusion: 'cancelled' }] };
const admitted = { conclusion: 'success', steps: [{ name: ADMISSION_STEP, conclusion: 'success' }] };

test('inline cancellation never falls through to tests, including read-only fork tokens', async () => {
  for (const status of [null, 403, 409]) {
    const calls = [];
    await cancelFromJob({
      github: { rest: { actions: { cancelWorkflowRun: async args => {
        calls.push(args); if (status) throw Object.assign(new Error('permission or race'), { status });
      } } } },
      context: { repo: { owner: 'base', repo: 'repo' }, runId: 7 },
      core: { info: () => {}, setFailed: message => calls.push(message) },
      wait: async ms => assert.equal(ms, 90000),
    });
    assert.deepEqual(calls[0], { owner: 'base', repo: 'repo', run_id: 7 });
    assert.match(calls[1], /no tests ran/);
  }
});

function fixture() {
  const run = { id: 7, event: 'pull_request', path: 'new-workflow.yml', status: 'in_progress', run_attempt: 1,
    head_sha: 'head', head_branch: 'branch', head_repository: { full_name: 'fork/repo' } };
  const pr = { head: { sha: 'head', ref: 'branch', repo: { full_name: 'fork/repo' } } };
  const state = { run, pr, jobs: [deferred], canceled: [], waits: 0 };
  const github = { rest: {
    actions: {
      getWorkflowRun: async () => ({ data: state.run }),
      listJobsForWorkflowRunAttempt: 'jobs',
      listWorkflowRunArtifacts: 'artifacts',
      cancelWorkflowRun: async args => state.canceled.push(args.run_id),
    }, pulls: { list: 'prs' },
  }, paginate: async (method, args) => {
    if (method === 'jobs') { assert.equal(args.attempt_number, 1); return state.jobs; }
    if (method === 'artifacts') return state.artifacts || [{ name: marker(7, 1) + 'test' }];
    return [state.pr];
  } };
  const context = { repo: { owner: 'base', repo: 'repo' }, payload: { workflow_run: { id: 7, run_attempt: 1 } } };
  return { state, github, context, wait: async () => { state.waits++; }, attempts: 2 };
}

test('trusted controller cancels marked fork runs and leaves admitted work running', async () => {
  const f = fixture(); await cancelDeferredRun(f); assert.deepEqual(f.state.canceled, [7]);
  const g = fixture(); g.state.jobs = [admitted]; await cancelDeferredRun(g);
  assert.deepEqual(g.state.canceled, []); assert.equal(g.state.waits, 0);
});

test('discovery requires a current deferral marker and matching PR head, repository and attempt', async () => {
  for (const patch of [{ head_sha: 'old' }, { run_attempt: 2 },
    { head_repository: { full_name: 'other/repo' } }, { event: 'push' }]) {
    const f = fixture(); Object.assign(f.state.run, patch); await cancelDeferredRun(f);
    assert.deepEqual(f.state.canceled, []);
  }
  const f = fixture();
  const args = { github: f.github, repo: f.context.repo, run: f.state.run, jobs: f.state.jobs };
  for (const artifact of [{ name: marker(7, 2) }, { name: marker(8, 1) },
    { name: marker(7, 1), expired: true }]) {
    f.state.artifacts = [artifact]; assert.equal(await readDecision(args), 'failed');
  }
  f.state.jobs = [{ steps: [{ name: 'Unrelated job' }] }];
  await cancelDeferredRun(f);
  assert.deepEqual(f.state.canceled, []);
  assert.equal(f.state.waits, 0);
});

test('both action imports work from an isolated action directory without a checkout', async () => {
  const fs = require('node:fs');
  const path = require('node:path');
  const root = fs.mkdtempSync(path.join(require('node:os').tmpdir(), 'ci-action-'));
  try {
    const action = path.join(root, 'action');
    fs.cpSync(__dirname, action, { recursive: true });
    const source = fs.readFileSync(path.join(action, 'action.yml'), 'utf8');
    const imports = [...source.matchAll(/require\(([^\n]+)\)/g)];
    assert.equal(imports.length, 2);
    for (const [, expression] of imports) {
      const load = new Function('require', 'process', `return require(${expression});`);
      const module = load(require, { env: { CI_ACTION_PATH: action, GITHUB_WORKSPACE: root } });
      const outputs = [];
      await module.checkDeferredCI({ context: { eventName: 'push' },
        core: { setOutput: (...args) => outputs.push(args) } });
      assert.deepEqual(outputs, [['run', 'true']]);
      assert.equal(typeof module.cancelFromJob, 'function');
    }
  } finally { fs.rmSync(root, { recursive: true, force: true }); }
});
