// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync, readdirSync } = require('node:fs');
const { join } = require('node:path');
const { afterHours, admission, candidates, reconcile, latestRuns, LABEL, STATUS, DEFERRED_STEP } = require('./');
const workflows = require('./workflows.json');
const day = new Date('2026-09-15T19:00:00Z'); // noon Pacific
const night = new Date('2026-09-16T04:00:00Z'); // 9pm Pacific
const pr = { number: 12, state: 'open', body: '[non-urgent]', labels: [],
  head: { sha: 'head', ref: 'feature', repo: { full_name: 'contributor/dolt' } } };
const context = { repo: { owner: 'dolthub', repo: 'dolt' }, serverUrl: 'https://github.com',
  eventName: 'pull_request', payload: { pull_request: pr } };
const run = { id: 100, path: workflows[0], event: 'pull_request', head_sha: 'head',
  head_branch: 'feature', head_repository: { full_name: 'contributor/dolt' }, pull_requests: [],
  status: 'completed', conclusion: 'success', run_attempt: 1, html_url: 'https://github.com/run/100' };
const postponed = [{ name: 'ci-admission / admission', conclusion: 'success',
  steps: [{ name: DEFERRED_STEP, conclusion: 'success' }] }];
const admitted = [{ name: 'ci-admission / admission', conclusion: 'success',
  steps: [{ name: DEFERRED_STEP, conclusion: 'skipped' }] }];

function fixture(options = {}) {
  const state = { pr: structuredClone(pr), runs: [structuredClone(run)], jobs: postponed,
    statuses: [], comments: [], fresh: null, calls: [], ...options };
  const method = (name, fn) => Object.assign(async args => {
    state.calls.push([name, args]);
    return { data: await fn(args) };
  }, { endpoint: name });
  const github = { rest: {
    pulls: {
      get: method('pulls.get', () => state.pr),
      list: method('pulls.list', () => [state.pr]),
    },
    repos: {
      listCommitStatusesForRef: method('statuses.list', () => state.statuses),
      createCommitStatus: method('statuses.create', args => state.statuses.unshift(args)),
    },
    issues: {
      listComments: method('comments.list', () => state.comments),
      createComment: method('comments.create', args => state.comments.push({ ...args, id: 1, user: { login: 'github-actions[bot]' } })),
      updateComment: method('comments.update', args => Object.assign(state.comments.find(c => c.id === args.comment_id), args)),
      getLabel: method('labels.get', () => ({ name: LABEL })),
      createLabel: method('labels.create', () => ({})),
      addLabels: method('labels.add', () => state.pr.labels = [{ name: LABEL }]),
      removeLabel: method('labels.remove', () => state.pr.labels = []),
    },
    actions: {
      listWorkflowRunsForRepo: method('runs.list', () => state.runs),
      listJobsForWorkflowRunAttempt: method('jobs.list', args => typeof state.jobs === 'function' ? state.jobs(args) : state.jobs),
      listJobsForWorkflowRun: method('jobs.all', () => state.allJobs || state.jobs),
      getWorkflowRun: method('runs.get', args => state.fresh || state.runs.find(r => r.id === args.run_id)),
      reRunWorkflow: method('runs.rerun', args => {
        const r = state.runs.find(r => r.id === args.run_id);
        r.status = 'queued'; r.run_attempt++;
      }),
    },
  }, paginate: async (endpoint, args) => (await endpoint(args)).data };
  return { state, github, calls: name => state.calls.filter(c => c[0] === name),
    reconcile: now => reconcile({ github, context, pullNumber: 12, now: now || day }) };
}

test('admission window includes 21:00 and excludes 05:00, including midnight and DST', () => {
  for (const [timestamp, expected] of [
    ['2026-09-16T03:59:59Z', false], ['2026-09-16T04:00:00Z', true],
    ['2026-09-16T07:00:00Z', true], ['2026-09-16T11:59:59Z', true], ['2026-09-16T12:00:00Z', false],
    ['2026-01-16T04:59:59Z', false], ['2026-01-16T05:00:00Z', true],
    ['2026-01-16T12:59:59Z', true], ['2026-01-16T13:00:00Z', false],
    ['2026-03-08T10:00:00Z', true], ['2026-11-01T09:00:00Z', true],
  ]) assert.equal(afterHours(new Date(timestamp)), expected, timestamp);
  assert.equal(afterHours(new Date('2026-09-16T21:00:00Z'), 'UTC'), true);
  assert.throws(() => afterHours(day, 'invalid/timezone'), RangeError);
});

test('gate reads live descriptions on initial runs and manual re-runs', async () => {
  const f = fixture();
  assert.deepEqual(await admission({ github: f.github, context, now: day }), { run: false, reason: 'deferred' });
  assert.equal((await admission({ github: f.github, context, now: night })).run, true);
  for (const body of [null, '', 'urgent', '[NON-URGENT]']) {
    f.state.pr.body = body;
    assert.equal((await admission({ github: f.github, context, now: day })).run, true);
  }
  f.state.pr.head.sha = 'new-head';
  assert.deepEqual(await admission({ github: f.github, context, now: night }), { run: false, reason: 'obsolete' });
  f.state.pr = { ...structuredClone(pr), state: 'closed' };
  assert.equal((await admission({ github: f.github, context, now: night })).run, false);
});

test('non-PR triggers retain normal behavior', async () => {
  assert.equal((await admission({ context: { eventName: 'workflow_dispatch' } })).run, true);
});

test('daytime deferral sets a pending check, tracks the queue, and comments once', async () => {
  const f = fixture();
  await f.reconcile(); await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.calls('runs.rerun').length, 0);
  assert.equal(f.calls('comments.create').length, 1);
  assert.equal(f.calls('comments.update').length, 0);
  assert.equal(f.calls('labels.add').length, 1);
  assert.equal(f.calls('jobs.list').length, 1, 'completed attempts are cached');
  assert.match(f.state.comments[0].body, /9pm and 5am \(America\/Los_Angeles\)/);
});

test('nighttime release re-runs the original workflow and waits for real results', async () => {
  const f = fixture();
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun')[0][1].run_id, 100);
  assert.equal(f.state.statuses[0].state, 'pending');
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 1);
  f.state.runs[0].status = 'completed';
  f.state.jobs = admitted;
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.calls('labels.remove').length, 1);
  assert.match(f.state.comments[0].body, /has finished/);
});

test('removing the marker releases daytime CI without a new commit', async () => {
  const f = fixture();
  await f.reconcile();
  f.state.pr.body = 'Now urgent';
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 1);
  assert.equal(f.state.statuses[0].state, 'pending');
});

test('pending status is initialized even before PR workflow runs appear', async () => {
  const f = fixture({ runs: [] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.calls('labels.add').length, 1);
});

test('does not retry failed tests or change which individual checks are required', async () => {
  const f = fixture({ jobs: admitted, pr: { ...structuredClone(pr), labels: [{ name: LABEL }] },
    runs: [{ ...run, conclusion: 'failure' }] });
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.state.runs[0].conclusion, 'failure');
  assert.match(f.state.statuses[0].description, /individual checks/);
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('gate errors fail closed even on ordinary urgent PRs', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), body: '' },
    jobs: [{ name: 'ci-admission / admission', conclusion: 'failure', steps: [] }],
    runs: [{ ...run, conclusion: 'failure' }] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'failure');
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('closed PRs and old revisions are never released', async () => {
  const closed = fixture({ pr: { ...structuredClone(pr), state: 'closed' } });
  await closed.reconcile(night);
  assert.equal(closed.calls('runs.rerun').length, 0);
  const old = fixture({ runs: [{ ...run, head_sha: 'old' }] });
  await old.reconcile(night);
  assert.equal(old.calls('runs.rerun').length, 0);
  assert.equal(old.state.statuses[0].state, 'pending');
});

test('only the newest run per workflow and the matching PR repository is eligible', () => {
  const current = { ...run, id: 101 };
  assert.deepEqual(latestRuns([run, current,
    { ...run, id: 102, head_repository: { full_name: 'someone/else' } },
    { ...run, id: 103, pull_requests: [{ number: 13 }] },
    { ...run, id: 104, path: '.github/workflows/not-ci.yaml' },
    { ...run, id: 105, event: 'workflow_dispatch' },
  ], pr), [current]);
});

test('a concurrent re-run cannot be released twice', async () => {
  const f = fixture({ fresh: { ...run, status: 'queued', run_attempt: 2 } });
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('a push between inspection and release prevents stale CI', async () => {
  const f = fixture();
  f.github.rest.pulls.get = async () => {
    if (f.calls('labels.add').length) f.state.pr.head.sha = 'new';
    return { data: f.state.pr };
  };
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('a partial release failure remains pending and can recover on the next timer', async () => {
  const f = fixture();
  const rerun = f.github.rest.actions.reRunWorkflow;
  f.github.rest.actions.reRunWorkflow = async () => { throw new Error('API unavailable'); };
  await assert.rejects(f.reconcile(night), /API unavailable/);
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.match(f.state.comments[0].body, /could not be released/);
  f.github.rest.actions.reRunWorkflow = rerun;
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 1);
});

test('fork workflow_run events without PR metadata are associated by head and repository', async () => {
  const f = fixture();
  assert.deepEqual(await candidates({ github: f.github,
    context: { ...context, eventName: 'workflow_run', payload: { workflow_run: run } } }), [12]);
  assert.deepEqual(await candidates({ github: f.github,
    context: { ...context, eventName: 'workflow_run', payload: { workflow_run: { ...run, event: 'push' } } } }), []);
});

test('timer recovers missed deferrals overnight; manual dispatch validates the PR number', async () => {
  const f = fixture();
  assert.deepEqual(await candidates({ github: f.github, context: { ...context, eventName: 'schedule' }, now: night }), [12]);
  assert.deepEqual(await candidates({ github: f.github, context: { ...context, eventName: 'schedule' }, now: day }), []);
  f.state.pr.body = '';
  f.state.pr.labels = [{ name: LABEL }];
  assert.deepEqual(await candidates({ github: f.github, context: { ...context, eventName: 'schedule' }, now: day }), [12]);
  assert.deepEqual(await candidates({ context: { ...context, eventName: 'workflow_dispatch' }, pullNumber: '12' }), [12]);
  await assert.rejects(candidates({ context: { ...context, eventName: 'workflow_dispatch' }, pullNumber: 'bad' }));
});

test('every PR workflow is registered and every test job depends on admission', () => {
  const dir = join(__dirname, '../../workflows');
  const actual = [];
  for (const file of readdirSync(dir)) {
    const source = readFileSync(join(dir, file), 'utf8');
    if (!/^  pull_request:/m.test(source)) continue;
    actual.push(`.github/workflows/${file}`);
    assert.match(source, /uses: .\/\.github\/workflows\/ci-admission.yaml/, file);
    const jobs = source.split(/^jobs:\n/m)[1].split(/(?=^  [\w-]+:\s*$)/m);
    for (const job of jobs) {
      if (!/^  [\w-]+:/m.test(job) || /^  ci-admission:/m.test(job)) continue;
      assert.match(job, /^    needs:.*ci-admission/m, `${file}: ${job.split('\n')[0]}`);
      assert.match(job, /^    if:.*needs.ci-admission.outputs.run == 'true'/m, file);
    }
  }
  assert.deepEqual(actual.sort(), [...workflows].sort());
  const scheduler = readFileSync(join(dir, 'ci-scheduler.yaml'), 'utf8');
  for (const file of workflows) {
    const name = readFileSync(join(dir, file.split('/').pop()), 'utf8').match(/^name: (.+)$/m)[1];
    assert.ok(scheduler.includes(JSON.stringify(name)), `Missing workflow_run subscription: ${name}`);
  }
});

test('all deferred suites must finish before the scheduling barrier clears', async () => {
  const f = fixture({ runs: [{ ...run }, { ...run, id: 101, path: workflows[1] }] });
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 2);
  f.state.runs[0].status = 'completed';
  f.state.jobs = admitted;
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'pending');
  f.state.runs[1].status = 'completed';
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'success');
});

test('marked PRs stay pending while admission jobs are queued', async () => {
  const f = fixture({ runs: [{ ...run, status: 'queued' }] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('a manually repeated deferral invalidates only that attempt in the cache', async () => {
  const f = fixture();
  await f.reconcile();
  f.state.runs[0].run_attempt++;
  await f.reconcile();
  assert.equal(f.calls('jobs.list').length, 2);
  assert.equal(f.calls('comments.create').length, 1);
});

test('does not read another user’s lookalike scheduling comment as a cache', async () => {
  const f = fixture({ comments: [{ id: 7, user: { login: 'contributor' },
    body: '<!-- dolt-ci-scheduling -->\nForged\n<!-- dolt-ci-state {"sha":"head","runs":{"100:1":"admitted"}} -->' }] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.calls('comments.create').length, 1);
  assert.equal(f.calls('comments.update').length, 0);
});

test('an invalid comment cache is rebuilt from live workflow attempts', async () => {
  const f = fixture({ comments: [{ id: 7, user: { login: 'github-actions[bot]' },
    body: '<!-- dolt-ci-scheduling -->\nWaiting\n<!-- dolt-ci-state invalid -->' }] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.calls('jobs.list').length, 1);
  assert.equal(f.calls('comments.create').length, 0);
  assert.equal(f.calls('comments.update').length, 1);
});

test('creates the queue label and tolerates another PR creating it concurrently', async () => {
  const f = fixture();
  f.github.rest.issues.getLabel = async () => { throw Object.assign(new Error('Not found'), { status: 404 }); };
  f.github.rest.issues.createLabel = async () => { throw Object.assign(new Error('Already exists'), { status: 422 }); };
  await f.reconcile();
  assert.equal(f.calls('labels.add').length, 1);
});

test('the 5am boundary is checked again immediately before release', async t => {
  const f = fixture();
  t.mock.method(Date, 'now', () => f.calls('labels.add').length ? 2000 : 0);
  await f.reconcile(new Date('2026-09-16T11:59:59Z'));
  assert.equal(f.calls('runs.rerun').length, 0);
  assert.equal(f.state.statuses[0].state, 'pending');
});

test('a canceled run that never started admission cannot clear the barrier', async () => {
  const f = fixture({ runs: [{ ...run, conclusion: 'cancelled' }], jobs: [] });
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'failure');
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('re-running only failed tests retains the earlier successful admission', async () => {
  const f = fixture({ runs: [{ ...run, run_attempt: 2, conclusion: 'failure' }],
    pr: { ...structuredClone(pr), labels: [{ name: LABEL }] },
    jobs: [{ id: 2, name: 'test', conclusion: 'failure', steps: [] }],
    allJobs: [{ ...admitted[0], id: 1 }] });
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.calls('jobs.all').length, 1);
  assert.equal(f.calls('runs.rerun').length, 0);
});
