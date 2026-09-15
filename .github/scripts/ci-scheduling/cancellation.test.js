// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const YAML = require('yaml');
const workflows = require('./workflows.json');
const { ADMISSION_STEP, DEFERRED_NOTICE, marker, readDecision, checkDeferredCI, jobDecision, cancelFromJob, cancelDeferredRun } = require('./cancellation');
const deferred = { conclusion: 'cancelled', check_run_url: 'https://api.github.com/repos/base/repo/check-runs/42',
  steps: [{ name: ADMISSION_STEP, conclusion: 'cancelled' }] };
const admitted = { conclusion: 'success', steps: [{ name: ADMISSION_STEP, conclusion: 'success' }] };

test('unmarked cancellations and admission errors fail closed while test failures remain admitted', () => {
  assert.equal(jobDecision([{ ...admitted, conclusion: 'failure' }]), 'admitted');
  assert.equal(jobDecision([{ conclusion: 'cancelled', steps: [] }]), 'failed');
  assert.equal(jobDecision([{ conclusion: 'failure', steps: [{ name: ADMISSION_STEP, conclusion: 'failure' }] }]), 'failed');
  assert.equal(jobDecision([{ conclusion: 'skipped', steps: [] }]), 'admitted');
  assert.equal(jobDecision([]), 'failed');
});

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
  const run = { id: 7, event: 'pull_request', path: workflows[0], status: 'in_progress', run_attempt: 1,
    head_sha: 'head', head_branch: 'branch', head_repository: { full_name: 'fork/repo' } };
  const pr = { head: { sha: 'head', ref: 'branch', repo: { full_name: 'fork/repo' } } };
  const state = { run, pr, jobs: [deferred], canceled: [], waits: 0 };
  const github = { rest: {
    actions: {
      getWorkflowRun: async () => ({ data: state.run }),
      listJobsForWorkflowRunAttempt: 'jobs',
      cancelWorkflowRun: async args => state.canceled.push(args.run_id),
    }, pulls: { list: 'prs' }, checks: { listAnnotations: 'annotations' },
  }, paginate: async (method, args) => {
    if (method === 'jobs') { assert.equal(args.attempt_number, 1); return state.jobs; }
    if (method === 'annotations') return state.annotations || [{ title: DEFERRED_NOTICE, message: marker(7, 1) }];
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

test('controller waits for the marker to appear after the run starts', async () => {
  const f = fixture(); f.state.jobs = [];
  f.wait = async () => { f.state.jobs = [deferred]; };
  await cancelDeferredRun(f); assert.deepEqual(f.state.canceled, [7]);
});

test('controller rejects obsolete heads, attempts, workflows, repositories and non-PR events', async () => {
  for (const patch of [{ head_sha: 'old' }, { run_attempt: 2 }, { path: 'unknown' },
    { head_repository: { full_name: 'different/repo' } }, { event: 'push' }, { status: 'completed' }]) {
    const f = fixture(); Object.assign(f.state.run, patch); await cancelDeferredRun(f);
    assert.deepEqual(f.state.canceled, []);
  }
});

test('each independent job uses one action and only failure handlers need guards', () => {
  for (const file of workflows) {
    const workflow = YAML.parse(readFileSync(join(__dirname, '../../..', file), 'utf8'));
    assert.ok(workflow.on.pull_request, file);
    assert.equal(workflow.on.workflow_call, undefined);
    assert.equal(workflow.jobs['ci-admission'], undefined);
    for (const [id, job] of Object.entries(workflow.jobs)) {
      const message = `${file}: ${id}`;
      const action = job.steps[0];
      assert.equal(action.name, ADMISSION_STEP, message);
      assert.equal(action.uses, '$/.github/actions/check-deferred-ci', message);
      assert.ok(Object.keys(action).length <= 4, message);
      for (const step of job.steps.slice(1)) {
        assert.notEqual(step.name, 'Checkout CI admission', message);
        if (/\b(always|failure|cancelled)\(/.test(step.if || '')) {
          assert.equal(action.id, 'ci-admission', message);
          assert.ok(step.if.includes("steps.ci-admission.outputs.run == 'true'"), message);
        } else assert.doesNotMatch(step.if || '', /ci-admission/, message);
      }
      assert.doesNotMatch(JSON.stringify(job.needs || []), /ci-admission/, message);
    }
  }
});

test('restricted jobs request cancellation permission instead of waiting for a queued controller', () => {
  for (const name of ['ci-check-correctness.yaml', 'ci-check-performance.yaml', 'ci-scheduling-tests.yaml']) {
    const workflow = YAML.parse(readFileSync(join(__dirname, '../../workflows', name), 'utf8'));
    for (const job of Object.values(workflow.jobs)) {
      assert.equal((job.permissions || workflow.permissions).actions, 'write', name);
    }
  }
});

test('only a deferral notice for the current attempt permits automatic release', async () => {
  const f = fixture();
  const args = { github: f.github, repo: f.context.repo, run: f.state.run, jobs: f.state.jobs };
  assert.equal(await readDecision(args), 'deferred');
  for (const notice of [{ title: DEFERRED_NOTICE, message: marker(7, 2) },
    { title: DEFERRED_NOTICE, message: marker(8, 1) }, { title: 'Other notice', message: marker(7, 1) }]) {
    f.state.annotations = [notice]; assert.equal(await readDecision(args), 'failed');
  }
  f.github.paginate = async () => { throw new Error('API failure'); };
  await assert.rejects(readDecision(args), /API failure/);
});

test('the shared action returns only for admitted work and records deferral before cancellation', async () => {
  for (const eventName of ['push', 'pull_request']) {
    const calls = [];
    const pr = { state: 'open', draft: true, head: { sha: 'head' }, labels: [] };
    const context = { eventName, repo: {}, runId: 7, payload: { pull_request: pr } };
    const github = { rest: { pulls: { get: async () => ({ data: pr }) }, actions: {
      cancelWorkflowRun: async () => { calls.push('cancel'); },
    } } };
    const core = { setOutput: (key, value) => calls.push([key, value]),
      notice: (message, opts) => calls.push([opts.title, message]),
      summary: { addRaw: () => ({ write: async () => {} }) },
      setFailed: () => calls.push('failed'),
    };
    await checkDeferredCI({ github, context, core, attempt: 1, wait: async () => {} });
    if (eventName === 'push') assert.deepEqual(calls, [['run', 'true']]);
    else assert.deepEqual(calls, [['run', 'false'], [DEFERRED_NOTICE, marker(7, 1)], 'cancel', 'failed']);
  }
});

test('the composite passes its action directory, timezone input and admission output explicitly', () => {
  const action = YAML.parse(readFileSync(join(__dirname, '../../actions/check-deferred-ci/action.yml'), 'utf8'));
  assert.equal(action.runs.using, 'composite');
  assert.equal(action.inputs.timezone.default, 'America/Los_Angeles');
  assert.equal(action.outputs.run.value, '${{ steps.admission.outputs.run }}');
  assert.equal(action.runs.steps[0].env.CI_ACTION_PATH, '${{ github.action_path }}');
  assert.equal(action.runs.steps[0].env.CI_TIMEZONE, '${{ inputs.timezone }}');
  assert.doesNotMatch(JSON.stringify(action), /vars\./);
});

test('admission API errors fail before cancellation or a deferral notice', async () => {
  const calls = [];
  await assert.rejects(checkDeferredCI({
    github: { rest: { pulls: { get: async () => { throw new Error('API unavailable'); } } } },
    context: { eventName: 'pull_request', repo: {}, payload: { pull_request: { number: 1 } } },
    core: { notice: () => calls.push('notice'), setOutput: () => calls.push('output') },
    attempt: 1,
  }), /API unavailable/);
  assert.deepEqual(calls, []);
});
