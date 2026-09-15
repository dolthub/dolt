// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const workflows = require('./workflows.json');
const MARKER = '[non-urgent]';
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

function nonUrgent(pr) {
  return (pr.body || '').includes(MARKER);
}

// Re-runs retain the old event payload. Always fetch the live description and head.
async function admission({ github, context, now, timezone = DEFAULT_TIMEZONE }) {
  if (context.eventName !== 'pull_request') return { run: true, reason: 'not-pr' };
  const { data: pr } = await github.rest.pulls.get({ ...context.repo,
    pull_number: context.payload.pull_request.number });
  if (pr.state !== 'open' || pr.head.sha !== context.payload.pull_request.head.sha) {
    return { run: false, reason: 'obsolete' };
  }
  const defer = nonUrgent(pr) && !afterHours(now, timezone);
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
  if (context.eventName === 'pull_request_target') return [context.payload.pull_request.number];
  if (context.eventName === 'workflow_run') {
    const run = context.payload.workflow_run;
    if (run.event !== 'pull_request') return [];
    // GitHub can omit pull_requests for fork runs. Match the repository and head too.
    const prs = await github.paginate(github.rest.pulls.list, { ...context.repo,
      state: 'open', per_page: 100 });
    return prs.filter(pr => belongsToPR(run, pr)).map(pr => pr.number);
  }
  // Scan descriptions as well as the queue label so a missed initial event cannot
  // strand a PR overnight. During the day only recover missed marker-removal events.
  const prs = await github.paginate(github.rest.pulls.list, { ...context.repo,
    state: 'open', sort: 'created', direction: 'asc', per_page: 100 });
  return prs.filter(pr => afterHours(now, timezone)
    ? nonUrgent(pr) || pr.labels.some(label => label.name === LABEL)
    : !nonUrgent(pr) && pr.labels.some(label => label.name === LABEL)).map(pr => pr.number);
}

async function reconcile({ github, context, pullNumber, now = new Date(), timezone = DEFAULT_TIMEZONE }) {
  const started = Date.now();
  const repo = context.repo;
  const { data: pr } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
  if (pr.state !== 'open') return;
  const sha = pr.head.sha;
  const tracked = pr.labels.some(label => label.name === LABEL);
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
  const comment = async text => { await loadComment(); message = text; };
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
    if (tracked || nonUrgent(pr) || previous?.state === 'pending') await loadComment();
    const runs = latestRuns(await github.paginate(github.rest.actions.listWorkflowRunsForRepo,
      { ...repo, event: 'pull_request', head_sha: sha, per_page: 100 }), pr);
    const deferred = [];
    let admissionFailed = false;
    const currentCache = {};
    for (const run of runs) {
      if (run.status !== 'completed') continue;
      // Ordinary PRs have no comment cache. On completion, inspect the triggering
      // run (and any failures) instead of repeatedly inspecting all successful runs.
      if (context.eventName === 'workflow_run' && !tracked && !nonUrgent(pr) &&
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
    const waiting = deferred.length > 0 || (runs.length === 0 && nonUrgent(pr) && !afterHours(now, timezone));
    if (waiting) {
      await status('pending', 'CI postponed; waiting for admission and test results');
      await track();
      await comment(`CI is postponed because this PR contains or previously contained \`${MARKER}\`. ` +
        `Deferred CI is released automatically between **9pm and 5am (${timezone})**. ` +
        `Remove the marker from the PR description to release it immediately. ` +
        `You can also remove the marker and run **Schedule PR CI** with PR number **${pullNumber}**, ` +
        `or use **Re-run all jobs** on the original CI runs. No test runner is held while waiting.`);
      let released = 0;
      for (const run of deferred) {
        // Recheck at each admission: edits, pushes, and the 5am boundary can race this loop.
        const { data: current } = await github.rest.pulls.get({ ...repo, pull_number: pullNumber });
        if (current.state !== 'open' || current.head.sha !== sha ||
            (nonUrgent(current) && !afterHours(new Date(now.getTime() + (Date.now() - started)), timezone))) break;
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
    if (tracked || previous?.state === 'pending' || nonUrgent(pr)) {
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
  DEFAULT_TIMEZONE, MARKER, LABEL, STATUS, DEFERRED_STEP };
