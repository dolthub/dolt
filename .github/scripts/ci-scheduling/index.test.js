// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { readFileSync, readdirSync } = require('node:fs');
const { join } = require('node:path');
const { ADMISSION_STEP, afterHours, admission, candidates, reconcile, latestRuns, hasApproval, AFTER_HOURS_LABEL, REVIEW_LABEL, FORCE_DRAFT_LABEL, LABEL, STATUS } = require('./');
const workflows = require('./workflows.json');
const { marker } = require('./cancellation');
const day = new Date('2026-09-15T19:00:00Z'); // noon Pacific
const night = new Date('2026-09-16T04:00:00Z'); // 9pm Pacific
const pr = { number: 12, state: 'open', draft: false, user: { login: 'author' }, body: '', labels: [{ name: AFTER_HOURS_LABEL }],
  head: { sha: 'head', ref: 'feature', repo: { full_name: 'contributor/dolt' } } };
const context = { repo: { owner: 'dolthub', repo: 'dolt' }, serverUrl: 'https://github.com',
  eventName: 'pull_request', payload: { pull_request: pr } };
const run = { id: 100, path: workflows[0], event: 'pull_request', head_sha: 'head',
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
      listWorkflowRunArtifacts: method('artifacts.list', args => [{
        name: marker(args.run_id, state.runs.find(run => run.id === args.run_id).run_attempt) + 'test' }]),
      listWorkflowRunsForRepo: method('runs.list', () => state.runs),
      listJobsForWorkflowRunAttempt: method('jobs.list', args =>
        (typeof state.jobs === 'function' ? state.jobs(args) : state.jobs).map(job => ({ ...job,
          check_run_url: `https://api.github.com/repos/dolthub/dolt/check-runs/${args.run_id}` }))),
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

test('gate reads live labels on initial runs and manual re-runs', async () => {
  const f = fixture();
  assert.deepEqual(await admission({ github: f.github, context, now: day }), { run: false, reason: 'deferred' });
  assert.equal((await admission({ github: f.github, context, now: night })).run, true);
  f.state.pr.labels = [];
  for (const body of [null, '', 'Ready for testing']) {
    f.state.pr.body = body;
    assert.equal((await admission({ github: f.github, context, now: day })).run, true);
  }
  f.state.pr.head.sha = 'new-head';
  assert.deepEqual(await admission({ github: f.github, context, now: night }), { run: false, reason: 'obsolete' });
  f.state.pr = { ...structuredClone(pr), state: 'closed' };
  assert.equal((await admission({ github: f.github, context, now: night })).run, false);
});

function review(id, state, login = 'reviewer', extra = {}) {
  return { id, state, user: { login }, submitted_at: `2026-09-15T${String(id).padStart(2, '0')}:00:00Z`, ...extra };
}

test('each deferral label is independent and combined labels require both conditions', async () => {
  for (const hours of [false, true]) {
    for (const needsReview of [false, true]) {
      for (const approved of [false, true]) {
        for (const now of [day, night]) {
          const labels = [hours && AFTER_HOURS_LABEL, needsReview && REVIEW_LABEL].filter(Boolean).map(name => ({ name }));
          const f = fixture({ pr: { ...structuredClone(pr), labels },
            reviews: approved ? [review(1, 'APPROVED')] : [] });
          const expected = (!hours || now === night) && (!needsReview || approved);
          const decision = await admission({ github: f.github, context, now });
          assert.equal(decision.run, expected, JSON.stringify({ hours, needsReview, approved, now }));
          assert.equal(f.calls('reviews.list').length, needsReview ? 1 : 0);
        }
      }
    }
  }
});

test('approvals respect later decisions and dismissals without treating comments as revocations', () => {
  assert.equal(hasApproval([review(1, 'PENDING')]), false);
  assert.equal(hasApproval([review(1, 'COMMENTED')]), false);
  assert.equal(hasApproval([review(1, 'APPROVED'), review(2, 'COMMENTED')]), true);
  assert.equal(hasApproval([review(1, 'APPROVED'), review(2, 'CHANGES_REQUESTED')]), false);
  assert.equal(hasApproval([review(1, 'APPROVED'), review(2, 'DISMISSED')]), false);
  assert.equal(hasApproval([review(2, 'APPROVED'), review(1, 'CHANGES_REQUESTED')]), true);
  assert.equal(hasApproval([review(1, 'APPROVED', 'first'), review(2, 'CHANGES_REQUESTED', 'second')]), true);
  assert.equal(hasApproval([review(1, 'APPROVED', 'author')], 'author'), false);
  assert.equal(hasApproval([{ ...review(1, 'APPROVED'), user: null }]), false);
  // A draft review may have an older ID but be submitted after another review.
  assert.equal(hasApproval([review(2, 'APPROVED'), review(1, 'CHANGES_REQUESTED', 'reviewer',
    { submitted_at: '2026-09-15T03:00:00Z' })]), false);
  // GitHub controls whether approvals are dismissed after a new commit.
  assert.equal(hasApproval([review(1, 'APPROVED', 'reviewer', { commit_id: 'older-head' })]), true);
});

test('draft creation holds tests before labels can be applied, even at night', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), draft: true, labels: [] } });
  assert.equal((await admission({ github: f.github, context, now: night })).run, false);
  // Label updates are separate requests, but the draft holds CI throughout them.
  f.state.pr.labels = [{ name: AFTER_HOURS_LABEL }, { name: REVIEW_LABEL }];
  assert.equal((await admission({ github: f.github, context, now: night })).run, false);
  f.state.pr.draft = false;
  assert.equal((await admission({ github: f.github, context, now: night })).run, false);
  f.state.reviews = [review(1, 'APPROVED')];
  assert.equal((await admission({ github: f.github, context, now: day })).run, false);
  assert.equal((await admission({ github: f.github, context, now: night })).run, true);
});

test('force-draft-ci bypasses the draft hold while preserving both deferral conditions', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), draft: true, labels: [{ name: FORCE_DRAFT_LABEL }] } });
  assert.equal((await admission({ github: f.github, context, now: day })).run, true);
  f.state.pr.labels.push({ name: AFTER_HOURS_LABEL }, { name: REVIEW_LABEL });
  assert.equal((await admission({ github: f.github, context, now: night })).run, false);
  f.state.reviews = [review(1, 'APPROVED')];
  assert.equal((await admission({ github: f.github, context, now: day })).run, false);
  assert.equal((await admission({ github: f.github, context, now: night })).run, true);
  f.state.pr.labels = [];
  assert.equal((await admission({ github: f.github, context, now: night })).run, false);
});

test('review API errors fail admission rather than admitting CI', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), labels: [{ name: REVIEW_LABEL }] } });
  f.github.rest.pulls.listReviews = async () => { throw new Error('Reviews unavailable'); };
  await assert.rejects(admission({ github: f.github, context, now: day }), /Reviews unavailable/);
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
  assert.equal(f.calls('jobs.list').length, 2, 'deferrals are read from live attempts');
  assert.match(f.state.comments[0].body, /9pm–5am \(America\/Los_Angeles\)/);
});

test('deferral comments document every applicable override, alone and combined', async () => {
  for (const draft of [false, true]) {
    for (const hours of [false, true]) {
      for (const needsReview of [false, true]) {
        if (!draft && !hours && !needsReview) continue;
        const labels = [hours && AFTER_HOURS_LABEL, needsReview && REVIEW_LABEL].filter(Boolean).map(name => ({ name }));
        const f = fixture({ pr: { ...structuredClone(pr), draft, labels } });
        await f.reconcile();
        const body = f.state.comments[0].body;
        assert.match(body, /How to override deferral/);
        assert.equal(body.includes('**Draft hold:** add `force-draft-ci` or mark the PR ready for review.'), draft);
        assert.equal(body.includes('**After-hours hold:** remove `defer-ci-after-hours`'), hours);
        assert.equal(body.includes('**Review hold:** remove `defer-ci-review`'), needsReview);
        assert.doesNotMatch(body, /Each override clears|For a manual retry|Manual retries/);
      }
    }
  }
});

test('a bypassed draft hold is not listed among the required overrides', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), draft: true,
    labels: [{ name: FORCE_DRAFT_LABEL }, { name: REVIEW_LABEL }] } });
  await f.reconcile();
  assert.match(f.state.comments[0].body, /\*\*Review hold:\*\* remove `defer-ci-review`/);
  assert.doesNotMatch(f.state.comments[0].body, /\*\*Draft hold:\*\*/);
});

test('deferral and release append events once without modifying earlier comments', async () => {
  const f = fixture();
  await f.reconcile();
  const deferred = f.state.comments[0].body;
  await f.reconcile();
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 1);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.calls('labels.remove').length, 1);
  assert.equal(f.state.runs[0].status, 'queued', 'release does not wait for tests');
  await f.reconcile(night);
  f.state.runs[0].status = 'completed';
  f.state.runs[0].conclusion = 'success';
  f.state.jobs = admitted;
  const inspections = f.calls('jobs.list').length;
  await f.reconcile(night);
  assert.equal(f.calls('jobs.list').length, inspections, 'successful tests are not inspected');
  assert.equal(f.state.comments.length, 2);
  assert.equal(f.state.comments[0].body, deferred);
  assert.match(f.state.comments[1].body, /Released postponed CI/);
  assert.doesNotMatch(f.state.comments[1].body, /override|finished|results|pending/);
  assert.equal(f.calls('comments.update').length, 0);
});

test('release API errors do not create comments or clear the hold', async () => {
  const f = fixture();
  await f.reconcile();
  const before = structuredClone(f.state.comments);
  f.github.rest.actions.reRunWorkflow = async () => { throw new Error('API unavailable'); };
  await assert.rejects(f.reconcile(night), /API unavailable/);
  assert.deepEqual(f.state.comments, before);
  assert.equal(f.state.statuses[0].state, 'pending');
});

test('a partial release logs the action and keeps unreleased work pending', async () => {
  const f = fixture({ runs: [{ ...run }, { ...run, id: 101, path: workflows[1] }] });
  const rerun = f.github.rest.actions.reRunWorkflow;
  f.github.rest.actions.reRunWorkflow = async args => {
    if (args.run_id === 101) throw new Error('API unavailable');
    return rerun(args);
  };
  await assert.rejects(f.reconcile(night), /API unavailable/);
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.state.comments.length, 1);
  assert.match(f.state.comments[0].body, /Released postponed CI/);
  f.github.rest.actions.reRunWorkflow = rerun;
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.state.comments.length, 1, 'late releases in the same episode do not spam');
});

test('removing the after-hours label releases daytime CI without a new commit', async () => {
  const f = fixture();
  await f.reconcile();
  f.state.pr.labels = [{ name: LABEL }];
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 1);
  assert.equal(f.state.statuses[0].state, 'success');
});

test('an approving review automatically releases review-only deferrals during the day', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), labels: [{ name: REVIEW_LABEL }] } });
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 0);
  assert.match(f.state.comments[0].body, /an approving review/);
  assert.doesNotMatch(f.state.comments[0].body, /9pm/);
  f.state.reviews = [review(1, 'APPROVED')];
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 1);
  // Approval is read again on release.
  assert.ok(f.calls('reviews.list').length >= 2);
});

test('an approval cannot bypass after-hours deferral when both labels are present', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), labels: [{ name: AFTER_HOURS_LABEL }, { name: REVIEW_LABEL }] } });
  await f.reconcile();
  assert.match(f.state.comments[0].body, /after-hours window/);
  assert.match(f.state.comments[0].body, /an approving review/);
  f.state.reviews = [review(1, 'APPROVED')];
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 0);
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 1);
});

test('removing one deferral label preserves the other label’s condition', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), labels: [{ name: AFTER_HOURS_LABEL }, { name: REVIEW_LABEL }] } });
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 0, 'nighttime does not replace an approval');
  f.state.pr.labels = [{ name: REVIEW_LABEL }, { name: LABEL }];
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 0, 'removing after-hours still requires a review');
  f.state.pr.labels = [{ name: AFTER_HOURS_LABEL }, { name: LABEL }];
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 0, 'removing review still requires after hours');
  f.state.pr.labels = [{ name: LABEL }];
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 1);
});

test('marking an unlabeled draft ready releases its postponed CI', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), draft: true, labels: [] } });
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 0);
  assert.match(f.state.comments[0].body, /draft PR to be marked ready/);
  f.state.pr.draft = false;
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 1);
});

test('adding force-draft-ci releases postponed draft CI without marking the PR ready', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), draft: true, labels: [] } });
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 0);
  assert.match(f.state.comments[0].body, /force-draft-ci/);
  f.state.pr.labels.push({ name: FORCE_DRAFT_LABEL });
  await f.reconcile();
  assert.equal(f.calls('runs.rerun').length, 1);
  assert.equal(f.state.pr.draft, true);
});

test('review dismissal between inspection and release is respected', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), labels: [{ name: REVIEW_LABEL }] }, reviews: [review(1, 'APPROVED')] });
  f.github.rest.pulls.get = async () => {
    if (f.calls('labels.add').length) f.state.reviews[0].state = 'DISMISSED';
    return { data: f.state.pr };
  };
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 0);
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
  assert.equal(f.state.comments.length, 0);
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('gate errors fail closed even on ordinary urgent PRs', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), labels: [] },
    jobs: [{ name: 'ci-admission / admission', conclusion: 'failure', steps: [] }],
    runs: [{ ...run, conclusion: 'failure' }] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'failure');
  assert.equal(f.calls('runs.rerun').length, 0);
  assert.equal(f.state.comments.length, 0);
});

test('closed PRs and old revisions are never released', async () => {
  const closed = fixture({ pr: { ...structuredClone(pr), state: 'closed' } });
  await closed.reconcile(night);
  assert.equal(closed.calls('runs.rerun').length, 0);
  const old = fixture({ runs: [{ ...run, head_sha: 'old' }] });
  await old.reconcile(night);
  assert.equal(old.calls('runs.rerun').length, 0);
  assert.equal(old.state.statuses[0].state, 'success');
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
  assert.equal(f.state.comments.length, 0);
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

test('review notifications, deferral label edits, and draft transitions reconcile the PR', async () => {
  const f = fixture();
  for (const action of ['labeled', 'unlabeled']) {
    for (const name of [AFTER_HOURS_LABEL, REVIEW_LABEL, FORCE_DRAFT_LABEL]) {
      assert.deepEqual(await candidates({ context: { ...context, eventName: 'pull_request_target',
        payload: { pull_request: pr, action, label: { name } } } }), [12]);
    }
    assert.deepEqual(await candidates({ context: { ...context, eventName: 'pull_request_target',
      payload: { pull_request: pr, action, label: { name: LABEL } } } }), []);
  }
  for (const action of ['ready_for_review', 'converted_to_draft']) {
    assert.deepEqual(await candidates({ context: { ...context, eventName: 'pull_request_target',
      payload: { pull_request: pr, action } } }), [12]);
  }
  for (const pull_requests of [[], [{ number: 12 }]]) {
    assert.deepEqual(await candidates({ github: f.github, context: { ...context,
      eventName: 'workflow_run', payload: { workflow_run: { ...run, event: 'pull_request_review', pull_requests } } } }), [12]);
  }
});

test('polling recovers review approvals and draft-ready transitions during daytime', async () => {
  const f = fixture({ pr: { ...structuredClone(pr), labels: [{ name: REVIEW_LABEL }] } });
  const schedule = { ...context, eventName: 'schedule' };
  assert.deepEqual(await candidates({ github: f.github, context: schedule, now: day }), [12]);
  f.state.pr.draft = true;
  f.state.pr.labels = [];
  assert.deepEqual(await candidates({ github: f.github, context: schedule, now: day }), [12]);
  f.state.pr.draft = false;
  f.state.pr.labels = [{ name: LABEL }];
  assert.deepEqual(await candidates({ github: f.github, context: schedule, now: day }), [12]);
});

test('every PR workflow is registered and every test job depends on admission', () => {
  const dir = join(__dirname, '../../workflows');
  const actual = [];
  for (const file of readdirSync(dir)) {
    const source = readFileSync(join(dir, file), 'utf8');
    if (!/^  pull_request:/m.test(source)) continue;
    actual.push(`.github/workflows/${file}`);
  }
  assert.deepEqual(actual.sort(), [...workflows].sort());
  const scheduler = readFileSync(join(dir, 'ci-scheduler.yaml'), 'utf8');
  for (const action of ['labeled', 'unlabeled', 'ready_for_review', 'converted_to_draft']) {
    assert.ok(scheduler.includes(action), `Missing PR activity: ${action}`);
  }
  assert.ok(scheduler.includes('"Notify CI review"'));
  assert.match(scheduler, /github.event.workflow_run.conclusion != 'success'/);
  assert.match(scheduler, /github.event.workflow_run.event == 'pull_request_review'/);
  const notification = readFileSync(join(dir, 'ci-review-notification.yaml'), 'utf8');
  assert.match(notification, /pull_request_review:/);
  assert.match(notification, /types: \[submitted, edited, dismissed\]/);
  assert.match(notification, /permissions: \{\}/);
  assert.doesNotMatch(notification, /actions\/checkout|secrets\./);
  for (const file of workflows) {
    const name = readFileSync(join(dir, file.split('/').pop()), 'utf8').match(/^name: (.+)$/m)[1];
    assert.ok(scheduler.includes(JSON.stringify(name)), `Missing workflow_run subscription: ${name}`);
  }
});

test('the scheduler can manage PR labels and comments even when repository issues are disabled', () => {
  const scheduler = readFileSync(join(__dirname, '../../workflows/ci-scheduler.yaml'), 'utf8');
  const reconcile = scheduler.split(/^  reconcile:\s*$/m)[1];
  const permissions = reconcile.match(/^    permissions:\n((?:^      .*\n)+)/m)[1];
  // PR metadata writes require PR permission; issue permission alone failed in the fork.
  assert.match(permissions, /^      pull-requests: write$/m);
  // Repository label creation still needs issue permission.
  assert.match(permissions, /^      issues: write$/m);
});

test('releasing all deferred suites clears the barrier before tests start', async () => {
  const f = fixture({ runs: [{ ...run }, { ...run, id: 101, path: workflows[1] }] });
  await f.reconcile(night);
  assert.equal(f.calls('runs.rerun').length, 2);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.ok(f.state.runs.every(run => run.status === 'queued'));
});

test('labeled PRs stay pending while admission jobs are queued', async () => {
  const f = fixture({ runs: [{ ...run, status: 'queued' }] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('a manually repeated deferral does not duplicate its event comment', async () => {
  const f = fixture();
  await f.reconcile();
  f.state.runs[0].run_attempt++;
  await f.reconcile();
  assert.equal(f.calls('jobs.list').length, 2);
  assert.equal(f.calls('comments.create').length, 1);
});

test("another user's lookalike event cannot suppress a deferral comment", async () => {
  const f = fixture({ comments: [{ id: 7, user: { login: 'contributor' },
    body: '<!-- dolt-ci-scheduling -->\nForged\n<!-- dolt-ci-event {"sha":"head","event":"deferred"} -->' }] });
  await f.reconcile();
  assert.equal(f.state.statuses[0].state, 'pending');
  assert.equal(f.calls('comments.create').length, 1);
  assert.equal(f.calls('comments.update').length, 0);
});

test('later deferrals and new revisions append events while preserving history', async () => {
  const f = fixture();
  await f.reconcile();
  await f.reconcile(night);
  f.state.runs[0].status = 'completed';
  await f.reconcile();
  assert.equal(f.state.comments.length, 3);
  f.state.pr.head.sha = 'new-head';
  f.state.runs[0].head_sha = 'new-head';
  await f.reconcile();
  assert.equal(f.state.comments.length, 4);
  assert.equal(f.calls('comments.update').length, 0);
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
  assert.equal(f.state.comments.length, 0);
});

test('successful workflow completions do not trigger reconciliation', async () => {
  const f = fixture();
  assert.deepEqual(await candidates({ github: f.github, context: { ...context,
    eventName: 'workflow_run', payload: { workflow_run: { ...run, conclusion: 'success' } } } }), []);
  assert.equal(f.calls('pulls.list').length, 0);
});

test('re-running only failed tests checks inline admission on that attempt', async () => {
  const f = fixture({ runs: [{ ...run, run_attempt: 2, conclusion: 'failure' }],
    pr: { ...structuredClone(pr), labels: [{ name: LABEL }] },
    jobs: [{ ...admitted[0], id: 2, conclusion: 'failure' }] });
  await f.reconcile(night);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.calls('jobs.all').length, 0);
  assert.equal(f.calls('runs.rerun').length, 0);
});

test('released work is not held again just because the time window closes', async () => {
  const f = fixture();
  await f.reconcile(night);
  await f.reconcile(day);
  assert.equal(f.state.statuses[0].state, 'success');
  assert.equal(f.state.comments.length, 1);
  assert.match(f.state.comments[0].body, /Released/);
});
