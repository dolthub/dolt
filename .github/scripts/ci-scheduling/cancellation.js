// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const workflows = require('./workflows.json');
const ADMISSION_STEP = 'Check CI admission';
const DEFERRED_STEP = 'CI postponed';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

function jobDecision(jobs) {
  const started = jobs.filter(job => job.conclusion !== 'skipped');
  if (started.some(job => job.steps?.some(step =>
    step.name === ADMISSION_STEP && step.conclusion === 'failure'))) return 'failed';
  // Canceling one matrix member cancels the workflow, including siblings that
  // may not have reached admission. A successful marker distinguishes this
  // deliberate cancellation from a user cancellation or a runner failure.
  if (started.some(job => job.steps?.some(step =>
    step.name === DEFERRED_STEP && step.conclusion === 'success'))) return 'deferred';
  if (started.some(job => !job.steps?.some(step =>
    step.name === ADMISSION_STEP && step.conclusion === 'success'))) return 'failed';
  return jobs.length ? 'admitted' : 'failed';
}

async function cancelFromJob({ github, context, core, wait = sleep }) {
  try {
    await github.rest.actions.cancelWorkflowRun({ ...context.repo, run_id: context.runId });
  } catch (error) {
    // Fork PRs and restricted jobs have read-only tokens. The trusted
    // workflow_run controller performs their cancellation instead.
    if (![403, 409].includes(error.status)) throw error;
    core.info('Waiting for the trusted CI scheduler to cancel this deferred run.');
  }
  // Never continue into tests while cancellation is asynchronous. Every actual
  // test/setup step is also guarded by the admission output. Bound runner time
  // if GitHub delays the cancellation controller; its completion event can still
  // record this marked run for later release.
  await wait(90000);
  core.setFailed('CI was deferred but cancellation did not arrive within 90 seconds; no tests ran.');
}

async function cancelDeferredRun({ github, context, wait = sleep, attempts = 20 }) {
  const runId = context.payload.workflow_run.id;
  const repo = context.repo;
  for (let attempt = 0; attempt < attempts; attempt++) {
    const { data: run } = await github.rest.actions.getWorkflowRun({ ...repo, run_id: runId });
    if (run.event !== 'pull_request' || !workflows.includes(run.path) || run.status === 'completed') return;
    if (run.run_attempt !== context.payload.workflow_run.run_attempt) return;
    // Only trust GitHub's metadata, never PR artifacts or code. Recheck the live
    // head so an old event cannot cancel the new revision's run.
    const prs = await github.paginate(github.rest.pulls.list, { ...repo, state: 'open', per_page: 100 });
    if (!prs.some(pr => pr.head.sha === run.head_sha && pr.head.ref === run.head_branch &&
        pr.head.repo?.full_name === run.head_repository?.full_name)) return;
    const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRunAttempt,
      { ...repo, run_id: run.id, attempt_number: run.run_attempt, per_page: 100 });
    if (jobDecision(jobs) === 'deferred') {
      try {
        await github.rest.actions.cancelWorkflowRun({ ...repo, run_id: run.id });
      } catch (error) {
        if (error.status !== 409) throw error; // A job or another controller already canceled it.
      }
      return;
    }
    if (jobs.length && jobs.every(job => job.conclusion === 'skipped' || job.steps?.some(step =>
      step.name === DEFERRED_STEP && step.conclusion === 'skipped'))) return;
    await wait(3000);
  }
}

module.exports = { ADMISSION_STEP, DEFERRED_STEP, jobDecision, cancelFromJob, cancelDeferredRun };
