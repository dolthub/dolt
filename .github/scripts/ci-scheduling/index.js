// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const workflows = require('./workflows.json');
const AFTER_HOURS_LABEL = 'defer-ci-after-hours';
const REVIEW_LABEL = 'defer-ci-review';
const FORCE_DRAFT_LABEL = 'force-draft-ci';
const LABEL = 'ci-deferred';
const STATUS = 'CI scheduling';
const COMMENT = '<!-- dolt-ci-scheduling -->';
const { ADMISSION_STEP, readDecision, cancelFromJob, cancelDeferredRun } = require('./cancellation');
const DEFAULT_TIMEZONE = 'America/Los_Angeles';

function afterHours(now = new Date(), timezone = DEFAULT_TIMEZONE) {
  const hour = Number(new Intl.DateTimeFormat('en-US', {
    timeZone: timezone, hour: 'numeric', hourCycle: 'h23',
  }).format(now));
  return hour >= 21 || hour < 5;
}

function hasLabel(pr, label) {
  return pr.labels.some(value => value.name === label);
}

function draftHeld(pr) {
  return pr.draft && !hasLabel(pr, FORCE_DRAFT_LABEL);
}

function hasDeferral(pr) {
  return draftHeld(pr) || hasLabel(pr, AFTER_HOURS_LABEL) || hasLabel(pr, REVIEW_LABEL);
}

function hasApproval(reviews, author) {
  // A comment-only review does not revoke an approval. Keep each reviewer's last
  // submitted decision, including dismissals and requests for changes.
  const decisions = new Map();
  const ordered = [...reviews].sort((a, b) =>
    (Date.parse(a.submitted_at) - Date.parse(b.submitted_at)) || a.id - b.id);
  for (const review of ordered) {
    if (!review.user || review.user.login === author ||
        !['APPROVED', 'CHANGES_REQUESTED', 'DISMISSED'].includes(review.state)) continue;
    decisions.set(review.user.login, review.state);
  }
  return [...decisions.values()].includes('APPROVED');
}

async function deferralReasons({ github, repo, pr, now, timezone = DEFAULT_TIMEZONE }) {
  const reasons = [];
  if (draftHeld(pr)) reasons.push('draft');
  if (hasLabel(pr, AFTER_HOURS_LABEL) && !afterHours(now, timezone)) reasons.push('after-hours');
  if (hasLabel(pr, REVIEW_LABEL)) {
    const reviews = await github.paginate(github.rest.pulls.listReviews,
      { ...repo, pull_number: pr.number, per_page: 100 });
    if (!hasApproval(reviews, pr.user?.login)) reasons.push('review');
  }
  return reasons;
}

function waitingMessage(reasons, timezone) {
  const conditions = [];
  if (reasons.includes('draft')) {
    conditions.push(`this draft PR to be marked ready for review or given the \`${FORCE_DRAFT_LABEL}\` label`);
  }
  if (reasons.includes('after-hours')) {
    conditions.push(`the after-hours window, **9pm–5am (${timezone})**, required by \`${AFTER_HOURS_LABEL}\``);
  }
  if (reasons.includes('review')) {
    conditions.push(`an approving review, required by \`${REVIEW_LABEL}\``);
  }
  const waiting = conditions.length ? `CI is postponed, waiting for ${conditions.join(' and ')}.`
    : 'Postponed CI is eligible to be released.';
  return waiting;
}

function overrideInstructions(pr) {
  const overrides = [];
  if (draftHeld(pr)) {
    overrides.push(`- **Draft hold:** add \`${FORCE_DRAFT_LABEL}\` or mark the PR ready for review.`);
  }
  if (hasLabel(pr, AFTER_HOURS_LABEL)) {
    overrides.push(`- **After-hours hold:** remove \`${AFTER_HOURS_LABEL}\` to allow CI during the day.`);
  }
  if (hasLabel(pr, REVIEW_LABEL)) {
    overrides.push(`- **Review hold:** remove \`${REVIEW_LABEL}\` to allow CI without an approving review.`);
  }
  if (overrides.length === 0) return '';
  return `\n\n**How to override deferral**\n\n${overrides.join('\n')}`;
}

// Re-runs retain the old event payload. Always fetch the live labels, reviews, and head.
async function admission({ github, context, now, timezone = DEFAULT_TIMEZONE }) {
  if (context.eventName !== 'pull_request') return { run: true, reason: 'not-pr' };
  const { data: pr } = await github.rest.pulls.get({ ...context.repo,
    pull_number: context.payload.pull_request.number });
  if (pr.state !== 'open' || pr.head.sha !== context.payload.pull_request.head.sha) {
    return { run: false, reason: 'obsolete' };
  }
  const reasons = await deferralReasons({ github, repo: context.repo, pr, now, timezone });
  const defer = reasons.length > 0;
  return { run: !defer, reason: defer ? 'deferred' : 'admitted' };
}

function belongsToPR(run, pr) {
  return run.event === 'pull_request' && run.head_sha === pr.head.sha &&
    run.head_repository?.full_name === pr.head.repo?.full_name &&
    run.head_branch === pr.head.ref &&
    (!run.pull_requests?.length || run.pull_requests.some(p => p.number === pr.number));
}

function latestRuns(runs, pr) {
  const latest = new Map();
  for (const run of runs) {
    if (!workflows.includes(run.path) || !belongsToPR(run, pr)) continue;
    if (!latest.has(run.path) || latest.get(run.path).id < run.id) latest.set(run.path, run);
  }
  return [...latest.values()];
}

async function candidates({ github, context, pullNumber, now, timezone = DEFAULT_TIMEZONE }) {
  if (context.eventName === 'workflow_dispatch') {
    const number = Number(pullNumber);
    if (!Number.isSafeInteger(number) || number < 1) throw new Error('Enter a positive PR number');
    return [number];
  }
  if (context.eventName === 'pull_request_target') {
    if (['labeled', 'unlabeled'].includes(context.payload.action) &&
        ![AFTER_HOURS_LABEL, REVIEW_LABEL, FORCE_DRAFT_LABEL].includes(context.payload.label?.name)) return [];
    return [context.payload.pull_request.number];
  }
  if (context.eventName === 'workflow_run') {
    const run = context.payload.workflow_run;
    if (!['pull_request', 'pull_request_review'].includes(run.event)) return [];
    if (run.event === 'pull_request' && run.conclusion === 'success') return [];
    // GitHub can omit pull_requests for fork runs. Match the repository and head too.
    const prs = await github.paginate(github.rest.pulls.list, { ...context.repo,
      state: 'open', per_page: 100 });
    if (run.event === 'pull_request_review') {
      // Review notifications carry no writable token or untrusted artifacts. The
      // controller identifies the PR and then re-fetches its actual review state.
      return prs.filter(pr => (run.pull_requests || []).some(p => p.number === pr.number) ||
        (run.head_branch === pr.head.ref &&
          run.head_repository?.full_name === pr.head.repo?.full_name)).map(pr => pr.number);
    }
    return prs.filter(pr => belongsToPR(run, pr)).map(pr => pr.number);
  }
  // Also scan requested deferrals so a missed initial event cannot strand a PR.
  // Review-only PRs and removed labels must be reconciled during daytime too.
  const prs = await github.paginate(github.rest.pulls.list, { ...context.repo,
    state: 'open', sort: 'created', direction: 'asc', per_page: 100 });
  return prs.filter(pr => hasDeferral(pr) || hasLabel(pr, LABEL)).filter(pr =>
    !hasLabel(pr, AFTER_HOURS_LABEL) || afterHours(now, timezone)).map(pr => pr.number);
}

async function reconcile({ github, context, pullNumber, now = new Date(), timezone = DEFAULT_TIMEZONE }) {
  const started = Date.now();
  const repo = context.repo;
  const { data: pr } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
  if (pr.state !== 'open') return;
  const sha = pr.head.sha;
  let tracked = hasLabel(pr, LABEL);
  const statuses = await github.paginate(github.rest.repos.listCommitStatusesForRef,
    { ...repo, ref: sha, per_page: 100 });
  const previous = statuses.find(status => status.context === STATUS);
  const status = async (state, description) => {
    // Never let an older reconciliation clear the latest revision's pending status.
    const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
    if (current.state !== 'open' || current.head.sha !== sha) return;
    if (previous?.state === state && previous?.description === description) return;
    await github.rest.repos.createCommitStatus({ ...repo, sha, context: STATUS, state, description,
      target_url: `${context.serverUrl}/${repo.owner}/${repo.repo}/actions/workflows/ci-scheduler.yaml` });
  };
  const comment = async (event, text) => {
    const comments = await github.paginate(github.rest.issues.listComments,
      { ...repo, issue_number: pullNumber, per_page: 100 });
    // Deduplicate repeated events for this revision without editing the event log.
    // This marker stores only the event identity, never workflow results.
    const events = comments.filter(c => c.user?.login === 'github-actions[bot]' &&
      c.body?.startsWith(COMMENT)).flatMap(c => {
      const saved = c.body.match(/<!-- dolt-ci-event (.+) -->/);
      try { return saved ? [JSON.parse(saved[1])] : []; } catch { return []; }
    }).filter(entry => entry.sha === sha);
    if (events.at(-1)?.event === event) return;
    await github.rest.issues.createComment({ ...repo, issue_number: pullNumber,
      body: `${COMMENT}\n${text}\n<!-- dolt-ci-event ${JSON.stringify({ sha, event })} -->` });
  };
  const track = async () => {
    if (tracked) return;
    try {
      await github.rest.issues.getLabel({ ...repo, name: LABEL });
    } catch (error) {
      if (error.status !== 404) throw error;
      try {
        await github.rest.issues.createLabel({ ...repo, name: LABEL, color: 'd4c5f9',
          description: 'CI postponed pending release' });
      } catch (error) {
        // Another PR may have created the repository label concurrently.
        if (error.status !== 422) throw error;
      }
    }
    await github.rest.issues.addLabels({ ...repo, issue_number: pullNumber, labels: [LABEL] });
    tracked = true;
  };

  const runs = latestRuns(await github.paginate(github.rest.actions.listWorkflowRunsForRepo,
    { ...repo, event: 'pull_request', head_sha: sha, per_page: 100 }), pr);
  const deferred = [];
  for (const run of runs) {
    // Inspect unsuccessful attempts only to distinguish canceled deferrals from
    // admission errors and ordinary test failures. Never monitor test completion.
    if (run.status !== 'completed' || run.conclusion === 'success') continue;
    const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRunAttempt,
      { ...repo, run_id: run.id, attempt_number: run.run_attempt, per_page: 100 });
    const decision = await readDecision({ github, repo, run, jobs });
    if (decision === 'failed') {
      await status('failure', 'CI admission failed; inspect and re-run the original workflows');
      return;
    }
    if (decision === 'deferred') deferred.push(run);
  }

  const reasons = await deferralReasons({ github, repo, pr, now, timezone });
  if (deferred.length > 0 || (reasons.length > 0 &&
      (runs.length === 0 || (previous?.state !== 'success' &&
        runs.some(run => run.status !== 'completed'))))) {
    await status('pending', 'CI postponed; waiting for release');
    await track();
    if (reasons.length > 0) {
      await comment('deferred', waitingMessage(reasons, timezone) + overrideInstructions(pr));
      return;
    }
    let released = 0;
    try {
      for (const run of deferred) {
        // Recheck at each release: edits, pushes, and the 5am boundary can race this loop.
        const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
        if (current.state !== 'open' || current.head.sha !== sha) break;
        const blockers = await deferralReasons({ github, repo, pr: current,
          now: new Date(now.getTime() + (Date.now() - started)), timezone });
        if (blockers.length > 0) break;
        const { data: fresh } = await github.rest.actions.getWorkflowRun({ ...repo, run_id: run.id });
        if (fresh.status !== 'completed' || fresh.run_attempt !== run.run_attempt) continue;
        // Re-runs preserve the original PR checks, merge ref, and fork permissions.
        // API errors remain in the scheduler logs, not the PR's event log.
        await github.rest.actions.reRunWorkflow({ ...repo, run_id: run.id });
        released++;
      }
    } finally {
      if (released > 0) await comment('released', 'Released postponed CI for this PR revision.');
    }
    if (released !== deferred.length) return;
  }
  // Admission ends at release. Existing required checks own all test results.
  await status('success', 'CI is not postponed');
  if (tracked) {
    await github.rest.issues.removeLabel({ ...repo, issue_number: pullNumber, name: LABEL });
  }
}

module.exports = { cancelFromJob, cancelDeferredRun, ADMISSION_STEP, afterHours, admission, candidates, reconcile, latestRuns,
  hasApproval, deferralReasons, DEFAULT_TIMEZONE, AFTER_HOURS_LABEL, REVIEW_LABEL, FORCE_DRAFT_LABEL, LABEL, STATUS };
