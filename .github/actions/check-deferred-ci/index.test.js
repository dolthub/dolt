// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { ADMISSION_STEP, afterHours, admission, candidates, reconcile, hasApproval, AFTER_HOURS_LABEL, REVIEW_LABEL, FORCE_DRAFT_LABEL, LABEL, STATUS } = require('./');
const { marker } = require('./cancellation');
const day = new Date('2026-09-15T19:00:00Z'); // noon Pacific
const night = new Date('2026-09-16T04:00:00Z'); // 9pm Pacific
const pr = { number: 12, state: 'open', draft: false, user: { login: 'author' }, body: '', labels: [{ name: AFTER_HOURS_LABEL }],
  head: { sha: 'head', ref: 'feature', repo: { full_name: 'contributor/dolt' } } };
const context = { repo: { owner: 'dolthub', repo: 'dolt' }, serverUrl: 'https://github.com',
  eventName: 'pull_request', payload: { pull_request: pr } };
const run = { id: 100, workflow_id: 10, path: '.github/workflows/new-name.yml', event: 'pull_request', head_sha: 'head',
  head_branch: 'feature', head_repository: { full_name: 'contributor/dolt' }, pull_requests: [],
  status: 'completed', conclusion: 'cancelled', run_attempt: 1, html_url: 'https://github.com/run/100' };
const postponed = [{ name: 'Go tests (ubuntu-22.04)', conclusion: 'cancelled',
  check_run_url: 'https://api.github.com/repos/dolthub/dolt/check-runs/1',
  steps: [{ name: ADMISSION_STEP, conclusion: 'cancelled' }] }];
const admitted = [{ name: 'Go tests (ubuntu-22.04)', conclusion: 'success',
  steps: [{ name: ADMISSION_STEP, conclusion: 'success' }] }];

function fixture(options = {}) {
  const state = { pr: structuredClone(pr), runs: [structuredClone(run)], jobs: postponed,
    statuses: [], comments: [], reviews: [], fresh: null, calls: [], ...options };
  const method = (name, fn) => Object.assign(async args => {
    state.calls.push([name, args]);
    return { data: await fn(args) };
  }, { endpoint: name });
  const github = { rest: {
    pulls: {
      get: method('pulls.get', () => state.pr),
      list: method('pulls.list', () => [state.pr]),
      listReviews: method('reviews.list', () => state.reviews),
    },
    repos: {
      listCommitStatusesForRef: method('statuses.list', () => state.statuses),
      createCommitStatus: method('statuses.create', args => state.statuses.unshift(args)),
    },
    issues: {
      listComments: method('comments.list', () => state.comments),
      createComment: method('comments.create', args => state.comments.push({ ...args, id: 1, user: { login: 'github-actions[bot]' } })),
      updateComment: method('comments.update', () => { throw new Error('Existing comments must never be edited'); }),
      getLabel: method('labels.get', () => ({ name: LABEL })),
      createLabel: method('labels.create', () => ({})),
      addLabels: method('labels.add', args => state.pr.labels.push(...args.labels.map(name => ({ name })))),
      removeLabel: method('labels.remove', args => state.pr.labels = state.pr.labels.filter(l => l.name !== args.name)),
    },
    actions: {
      listWorkflowRunArtifacts: method('artifacts.list', args => state.artifacts || [{
        name: marker(args.run_id, state.runs.find(run => run.id === args.run_id).run_attempt) + 'test' }]),
      listWorkflowRunsForRepo: method('runs.list', () => state.runs),
      listJobsForWorkflowRunAttempt: method('jobs.list', args =>
        (typeof state.jobs === 'function' ? state.jobs(args) : state.jobs).map(job => ({ ...job,
          check_run_url: `https://api.github.com/repos/dolthub/dolt/check-runs/${args.run_id}` }))),
      getWorkflowRun: method('runs.get', args => state.fresh || state.runs.find(r => r.id === args.run_id)),
      reRunWorkflow: method('runs.rerun', args => {
        const r = state.runs.find(r => r.id === args.run_id);
        r.status = 'queued'; r.run_attempt++;
      }),
    },
  }, paginate: async (endpoint, args) => (await endpoint(args)).data };
  return { state, github, calls: name => state.calls.filter(c => c[0] === name),
    reconcile: (now, eventContext = context) => reconcile({ github, context: eventContext, pullNumber: 12, now: now || day }) };
}

test('admission composes draft, after-hours and review conditions using live PR state', async () => {
  assert.equal(afterHours(new Date('2026-09-16T03:59:59Z')), false);
  assert.equal(afterHours(night), true);
  assert.equal(afterHours(new Date('2026-09-16T12:00:00Z')), false);
  const f = fixture({ pr: { ...structuredClone(pr), draft: true,
    labels: [{ name: AFTER_HOURS_LABEL }, { name: REVIEW_LABEL }] } });
  const allowed = async now => (await admission({ github: f.github, context, now })).run;
  assert.equal(await allowed(night), false);
  f.state.pr.labels.push({ name: FORCE_DRAFT_LABEL });
  assert.equal(await allowed(night), false);
  f.state.reviews = [{ id: 1, user: { login: 'reviewer' }, state: 'APPROVED' }];
  assert.equal(await allowed(day), false);
  assert.equal(await allowed(night), true);
  f.state.pr.labels = [];
  assert.equal(await allowed(night), false);
  f.state.pr.draft = false;
  assert.equal(await allowed(day), true);
  f.state.pr.head.sha = 'old';
  assert.equal(await allowed(day), false);
  assert.equal((await admission({ context: { eventName: 'push' } })).run, true);
  assert.equal(hasApproval([...f.state.reviews,
    { id: 2, user: { login: 'reviewer' }, state: 'DISMISSED' }]), false);
});

test('new and renamed workflows are discovered from the PR; unrelated runs are ignored', async () => {
  const f = fixture({ runs: [run, { ...run, id: 101, path: 'renamed.yml' },
    { ...run, id: 102, workflow_id: 11 }, { ...run, id: 103, workflow_id: 12 },
    { ...run, id: 104, head_sha: 'old' }, { ...run, id: 105, event: 'push' },
    { ...run, id: 106, head_repository: { full_name: 'other/repo' } }],
    jobs: args => args.run_id === 103 ? [{ steps: [{ name: 'Unrelated job' }] }] : postponed });
  await f.reconcile(night);
  assert.deepEqual(f.calls('runs.rerun').map(([, args]) => args.run_id), [101, 102]);
  assert.equal(f.state.statuses[0].state, 'success');
});

test('relevant label changes append deferral and release comments once across commits', async () => {
  const f = fixture();
  const labeled = { ...context, eventName: 'pull_request_target',
    payload: { action: 'labeled', label: { name: AFTER_HOURS_LABEL } } };
  await f.reconcile(day, labeled);
  f.state.pr.head.sha = f.state.runs[0].head_sha = 'new-head';
  await f.reconcile(day, labeled);
  const original = f.state.comments[0].body;
  assert.match(original, /9pm–5am/);
  assert.match(original, /remove `defer-ci-after-hours`/);
  assert.equal(f.state.statuses[0].state, 'pending');
  f.state.pr.labels = [{ name: LABEL }];
  const unlabeled = { ...labeled, payload: { ...labeled.payload, action: 'unlabeled' } };
  await f.reconcile(day, unlabeled); await f.reconcile(day, unlabeled);
  assert.equal(f.calls('runs.rerun').length, 1);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.state.runs[0].status, 'queued');
  assert.equal(f.calls('labels.remove').length, 1);
  f.state.runs[0].status = 'completed'; f.state.runs[0].conclusion = 'success';
  await f.reconcile();
  assert.equal(f.state.comments.length, 2);
  assert.equal(f.state.comments[0].body, original);
  assert.match(f.state.comments[1].body, /Released postponed CI/);
  assert.doesNotMatch(f.state.comments[1].body, /finished|results|override/);
});

test('release errors stay pending, and ordinary test failures are never retried or commented on', async () => {
  const f = fixture();
  f.github.rest.actions.reRunWorkflow = async () => { throw new Error('API error'); };
  await assert.rejects(f.reconcile(night), /API error/);
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.state.comments.length, 0);
  f.state.jobs = admitted;
  f.state.runs[0].conclusion = 'failure';
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.state.comments.length, 0);
  f.state.jobs = postponed; f.state.artifacts = [];
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'failure');
});

test('release rechecks the current head and conditions immediately before rerunning', async () => {
  for (const change of [f => f.state.pr.head.sha = 'new',
    f => f.state.pr.draft = true,
    f => f.state.fresh = { ...run, run_attempt: 2 }]) {
    const f = fixture();
    f.github.rest.pulls.get = async () => {
      if (f.calls('labels.add').length) change(f);
      return { data: f.state.pr };
    };
    await f.reconcile(night);
    assert.equal(f.calls('runs.rerun').length, 0);
  }
});

test('review notifications and polling wake reconciliation; successful CI does not', async () => {
  const f = fixture();
  const event = run => ({ ...context, eventName: 'workflow_run', payload: { workflow_run: run } });
  assert.deepEqual(await candidates({ github: f.github, context: event({ ...run,
    event: 'pull_request_review', conclusion: 'success' }) }), [12]);
  assert.deepEqual(await candidates({ github: f.github, context: event({ ...run, conclusion: 'success' }) }), []);
  assert.deepEqual(await candidates({ github: f.github, context: { ...context, eventName: 'schedule' }, now: night }), [12]);
  assert.deepEqual(await candidates({ github: f.github, context: { ...context, eventName: 'schedule' }, now: day }), []);
});

test('pushes and other events without label or draft changes reconcile without reading or adding comments', async () => {
  const contexts = [
    ...['opened', 'synchronize', 'reopened'].map(action =>
      ({ ...context, eventName: 'pull_request_target', payload: { action } })),
    ...[LABEL, 'unrelated'].map(name => ({ ...context, eventName: 'pull_request_target',
      payload: { action: 'labeled', label: { name } } })),
    ...['schedule', 'workflow_dispatch', 'workflow_run'].map(eventName => ({ ...context, eventName })),
  ];
  for (const eventContext of contexts) {
    const f = fixture({ pr: { ...structuredClone(pr), draft: true } });
    await f.reconcile(day, eventContext);
    f.state.pr.head.sha = f.state.runs[0].head_sha = 'new-head';
    await f.reconcile(day, eventContext);
    assert.equal(f.state.statuses[0].state, 'pending');
    f.state.pr.draft = false;
    await f.reconcile(night, eventContext);
    assert.equal(f.calls('runs.rerun').length, 1);
    assert.equal(f.calls('comments.list').length, 0);
    assert.equal(f.state.comments.length, 0);
  }
  for (const name of [REVIEW_LABEL, FORCE_DRAFT_LABEL]) {
    const f = fixture({ pr: { ...structuredClone(pr), draft: true, labels: [{ name: REVIEW_LABEL }] } });
    await f.reconcile(day, { ...context, eventName: 'pull_request_target',
      payload: { action: 'labeled', label: { name } } });
    assert.equal(f.state.comments.length, 1);
  }
});

test('draft-ready transitions comment while commits on a draft remain silent', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), draft: true, labels: [] } });
  const event = action => ({ ...context, eventName: 'pull_request_target', payload: { action } });
  await f.reconcile(day, event('converted_to_draft'));
  assert.equal(f.state.comments.length, 1);
  f.state.pr.head.sha = f.state.runs[0].head_sha = 'new-head';
  await f.reconcile(day, event('synchronize'));
  assert.equal(f.state.comments.length, 1);
  f.state.pr.draft = false;
  await f.reconcile(day, event('ready_for_review'));
  assert.equal(f.calls('runs.rerun').length, 1);
  assert.equal(f.state.comments.length, 2);
  assert.match(f.state.comments[1].body, /Released postponed CI/);
  await f.reconcile(day, event('ready_for_review'));
  assert.equal(f.state.comments.length, 2);
});
