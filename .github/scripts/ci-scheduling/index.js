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
const DEFERRED_STEP = 'CI postponed';
const GATE_JOB = 'ci-admission / admission';
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
  return `${waiting} No test runner is held while waiting.`;
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
  return `\n\n**How to override deferral**\n\n${overrides.join('\n')}\n\n` +
    'Each override clears only its own condition; satisfy or override every remaining condition to start CI. ' +
    'These changes trigger an automatic recheck. For a manual retry, run **Schedule PR CI** ' +
    `with PR number **${pr.number}**, or use **Re-run all jobs** on a postponed workflow. ` +
    'Manual retries recheck the draft state, labels, and reviews; they do not bypass outstanding conditions.';
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
  const tracked = hasLabel(pr, LABEL);
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
  let existingComment;
  let commentsLoaded = false;
  let message;
  let cache = { sha, runs: {} };
  let cacheChanged = false;
  const loadComment = async () => {
    if (commentsLoaded) return;
    commentsLoaded = true;
    const comments = await github.paginate(github.rest.issues.listComments,
      { ...repo, issue_number: pullNumber, per_page: 100 });
    existingComment = comments.find(c => c.user?.login === 'github-actions[bot]' && c.body?.startsWith(COMMENT));
    const saved = existingComment?.body.match(/<!-- dolt-ci-state (.+) -->/);
    if (saved) {
      try {
        const parsed = JSON.parse(saved[1]);
        if (parsed.sha === sha && parsed.runs && typeof parsed.runs === 'object') cache = parsed;
      } catch { /* A missing or damaged cache is rebuilt from the Actions API. */ }
    }
  };
  const comment = async text => {
    await loadComment();
    // Preserve actionable overrides when release progress or errors replace the
    // waiting message in the same bot comment.
    message = text + overrideInstructions(pr);
  };
  const saveComment = async () => {
    if (!message && !(existingComment && cacheChanged)) return;
    // The cache avoids re-fetching every completed job for every workflow_run event.
    // Only our own bot's comment is read, and IDs/attempts still come from the live API.
    const text = message || existingComment.body.split('\n<!-- dolt-ci-state ')[0].slice(COMMENT.length + 1);
    const body = `${COMMENT}\n${text}\n<!-- dolt-ci-state ${JSON.stringify(cache)} -->`;
    if (existingComment && existingComment.body !== body) {
      await github.rest.issues.updateComment({ ...repo, comment_id: existingComment.id, body });
    } else if (!existingComment) {
      await github.rest.issues.createComment({ ...repo, issue_number: pullNumber, body });
    }
  };
  const track = async () => {
    if (tracked) return;
    try {
      await github.rest.issues.getLabel({ ...repo, name: LABEL });
    } catch (error) {
      if (error.status !== 404) throw error;
      try {
        await github.rest.issues.createLabel({ ...repo, name: LABEL, color: 'd4c5f9',
          description: 'CI postponed or running after postponement' });
      } catch (error) {
        // Another PR may have created the repository label concurrently.
        if (error.status !== 422) throw error;
      }
    }
    await github.rest.issues.addLabels({ ...repo, issue_number: pullNumber, labels: [LABEL] });
  };

  try {
    if (tracked || hasDeferral(pr) || previous?.state === 'pending') await loadComment();
    const runs = latestRuns(await github.paginate(github.rest.actions.listWorkflowRunsForRepo,
      { ...repo, event: 'pull_request', head_sha: sha, per_page: 100 }), pr);
    const deferred = [];
    let admissionFailed = false;
    const currentCache = {};
    for (const run of runs) {
      if (run.status !== 'completed') continue;
      // Ordinary PRs have no comment cache. On completion, inspect the triggering
      // run (and any failures) instead of repeatedly inspecting all successful runs.
      if (context.eventName === 'workflow_run' && !tracked && !hasDeferral(pr) &&
          previous?.state !== 'pending' && run.conclusion === 'success' &&
          run.id !== context.payload.workflow_run.id) continue;
      const key = `${run.id}:${run.run_attempt}`;
      let decision = cache.runs[key];
      if (!['deferred', 'admitted', 'failed'].includes(decision)) {
        const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRunAttempt,
          { ...repo, run_id: run.id, attempt_number: run.run_attempt, per_page: 100 });
        let gate = jobs.find(job => job.name === GATE_JOB);
        if (!gate && run.run_attempt > 1) {
          // Re-running only failed tests may omit the already-successful gate from
          // the new attempt. Its most recent execution still controls admission.
          const allJobs = await github.paginate(github.rest.actions.listJobsForWorkflowRun,
            { ...repo, run_id: run.id, filter: 'all', per_page: 100 });
          gate = allJobs.filter(job => job.name === GATE_JOB).sort((a, b) => b.id - a.id)[0];
        }
        if (!gate) {
          // Existing runs from before rollout have no gate. A missing gate must
          // not clear the barrier on a canceled or otherwise unsuccessful run.
          decision = run.conclusion === 'success' ? 'admitted' : 'failed';
        } else if (gate.conclusion !== 'success') {
          decision = 'failed';
        } else if (gate.steps?.some(step => step.name === DEFERRED_STEP && step.conclusion === 'success')) {
          decision = 'deferred';
        } else {
          decision = 'admitted';
        }
        cacheChanged = true;
      }
      currentCache[key] = decision;
      if (decision === 'failed') admissionFailed = true;
      if (decision === 'deferred') deferred.push(run);
    }
    cache.runs = currentCache;

    if (admissionFailed) {
      await status('failure', 'CI admission failed; inspect and re-run the original workflows');
      await track();
      await comment('CI admission failed. Inspect and re-run the original CI workflows.');
      return;
    }

    // Initialize the pending status before path-filtered CI workflows have appeared.
    const reasons = await deferralReasons({ github, repo, pr, now, timezone });
    const waiting = deferred.length > 0 || (runs.length === 0 && reasons.length > 0);
    if (waiting) {
      await status('pending', 'CI postponed; waiting for admission and test results');
      await track();
      await comment(waitingMessage(reasons, timezone));
      let released = 0;
      for (const run of deferred) {
        // Recheck at each admission: edits, pushes, and the 5am boundary can race this loop.
        const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
        if (current.state !== 'open' || current.head.sha !== sha) break;
        const blockers = await deferralReasons({ github, repo, pr: current,
          now: new Date(now.getTime() + (Date.now() - started)), timezone });
        if (blockers.length > 0) break;
        const { data: fresh } = await github.rest.actions.getWorkflowRun({ ...repo, run_id: run.id });
        if (fresh.status !== 'completed' || fresh.run_attempt !== run.run_attempt) continue;
        // Re-runs preserve the original PR checks, merge ref, and fork permission restrictions.
        try {
          await github.rest.actions.reRunWorkflow({ ...repo, run_id: run.id });
          released++;
        } catch (error) {
          await comment(`CI could not be released for [workflow run ${run.id}](${run.html_url}). ` +
            `Inspect **Schedule PR CI** for the API error. GitHub limits re-runs to 30 days and 50 attempts; ` +
            `if that limit was reached, push a new commit to create fresh PR workflows.`);
          throw error;
        }
      }
      if (released > 0) await comment(`Released ${released} postponed CI workflow(s) for the current PR revision. ` +
        `The original workflow runs show progress; **CI scheduling** stays pending until CI completes.`);
      return;
    }
    if (tracked || previous?.state === 'pending' || hasDeferral(pr)) {
      if (runs.length === 0 || runs.some(run => run.status !== 'completed')) {
        await status('pending', 'Waiting for CI admission and test results');
        await track();
        return;
      }
      // CI scheduling is an admission barrier, not a replacement for the existing
      // required test checks. Optional test failures must not become mandatory.
      await status('success', 'Postponed CI finished; see individual checks for results');
      await comment('Postponed CI has finished for the current PR revision. ' +
        'See the individual workflow checks for test results; failed tests are not automatically retried.');
      if (tracked) await github.rest.issues.removeLabel({ ...repo, issue_number: pullNumber, name: LABEL });
    } else {
      await status('success', 'CI is not postponed');
    }
  } finally {
    await saveComment();
  }
}

module.exports = { afterHours, admission, candidates, reconcile, latestRuns,
  hasApproval, deferralReasons, DEFAULT_TIMEZONE, AFTER_HOURS_LABEL, REVIEW_LABEL, FORCE_DRAFT_LABEL, LABEL, STATUS, DEFERRED_STEP };
