// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const YAML = require('yaml');
const workflows = require('./workflows.json');
const { ADMISSION_STEP, DEFERRED_STEP, jobDecision, cancelFromJob, cancelDeferredRun } = require('./cancellation');
const deferred = { conclusion: 'cancelled', steps: [
  { name: ADMISSION_STEP, conclusion: 'success' }, { name: DEFERRED_STEP, conclusion: 'success' },
] };
const admitted = { conclusion: 'success', steps: [{ name: ADMISSION_STEP, conclusion: 'success' }, { name: DEFERRED_STEP, conclusion: 'skipped' }] };

test('deliberate cancellation is distinguished from user cancellation, test failure, and admission errors', () => {
  assert.equal(jobDecision([deferred, { conclusion: 'cancelled', steps: [] }]), 'deferred');
  assert.equal(jobDecision([{ ...admitted, conclusion: 'failure' }]), 'admitted');
  assert.equal(jobDecision([{ conclusion: 'cancelled', steps: [] }]), 'failed');
  assert.equal(jobDecision([{ conclusion: 'failure', steps: [{ name: ADMISSION_STEP, conclusion: 'failure' }] }, deferred]), 'failed');
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
    }, pulls: { list: 'prs' },
  }, paginate: async (method, args) => {
    if (method === 'jobs') { assert.equal(args.attempt_number, 1); return state.jobs; }
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

test('each independent job checks admission before setup and guards every original step', () => {
  for (const file of workflows) {
    const workflow = YAML.parse(readFileSync(join(__dirname, '../../..', file), 'utf8'));
    assert.ok(workflow.on.pull_request, file);
    assert.equal(workflow.on.workflow_call, undefined);
    assert.equal(workflow.jobs['ci-admission'], undefined);
    for (const [id, job] of Object.entries(workflow.jobs)) {
      const message = `${file}: ${id}`;
      assert.deepEqual(job.steps.slice(0, 4).map(s => s.name),
        ['Checkout CI admission', ADMISSION_STEP, DEFERRED_STEP, 'Cancel deferred CI'], message);
      assert.equal(job.steps[0].with['persist-credentials'], false, message);
      assert.equal(job.steps[2].if, "steps.ci-admission.outputs.reason == 'deferred'", message);
      assert.equal(job.steps[3].if, "github.event_name == 'pull_request' && steps.ci-admission.outputs.run == 'false'", message);
      for (const step of job.steps.slice(4)) {
        assert.ok(step.if.includes("github.event_name != 'pull_request' || steps.ci-admission.outputs.run == 'true'"), message);
      }
      assert.doesNotMatch(JSON.stringify(job.needs || []), /ci-admission/, message);
    }
  }
});
