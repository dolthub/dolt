// Copyright 2026 Dolthub, Inc.
// Licensed under the Apache License, Version 2.0.
'use strict';

const ADMISSION_STEP = 'Check deferred CI';
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const marker = (run, attempt) => `dolt-ci-deferred-${run}-${attempt}-`;

async function recordDeferral(run, attempt) {
  const { mkdtemp, writeFile } = require('node:fs/promises');
  const { join } = require('node:path');
  const { tmpdir } = require('node:os');
  const { randomUUID } = require('node:crypto');
  const path = join(await mkdtemp(join(tmpdir(), 'dolt-ci-')), 'deferred.json');
  await writeFile(path, JSON.stringify({ run, attempt }));
  return { path, name: marker(run, attempt) + randomUUID() };
}

async function checkDeferredCI({ github, context, core, timezone, attempt, record = recordDeferral }) {
  const { admission } = require('./index');
  const decision = await admission({ github, context, timezone });
  core.setOutput('run', String(decision.run));
  if (decision.reason === 'deferred') {
    // The action uploads this tiny marker before waiting for cancellation.
    // Unlike check annotations it is visible while the action is still running.
    const artifact = await record(context.runId, attempt);
    core.setOutput('marker-path', artifact.path);
    core.setOutput('marker-name', artifact.name);
    await core.summary.addRaw('CI postponed. See the PR comment for conditions and overrides.').write();
  }
}

function jobDecision(jobs) {
  const started = jobs.filter(job => job.conclusion !== 'skipped');
  if (started.some(job => !job.steps?.some(step =>
    step.name === ADMISSION_STEP && step.conclusion === 'success'))) return 'failed';
  return jobs.length ? 'admitted' : 'failed';
}

async function readDecision({ github, repo, run, jobs }) {
  // Discover participation from GitHub's job metadata, not a workflow registry.
  if (!jobs.some(job => job.steps?.some(step => step.name === ADMISSION_STEP))) return 'unmanaged';
  const decision = jobDecision(jobs);
  if (decision === 'admitted' || !jobs.some(job => job.steps?.some(step =>
    step.name === ADMISSION_STEP && step.conclusion !== 'skipped'))) return decision;
  const artifacts = await github.paginate(github.rest.actions.listWorkflowRunArtifacts,
    { ...repo, run_id: run.id, per_page: 100 });
  if (artifacts.some(artifact => !artifact.expired && artifact.name.startsWith(marker(run.id, run.run_attempt)))) {
    return 'deferred';
  }
  return decision;
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
  // Never return successfully while cancellation is asynchronous. Ordinary
  // steps require success; failure/always handlers retain an admission guard.
  // Bound runner time
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
    if (run.event !== 'pull_request' || run.status === 'completed') return;
    if (run.run_attempt !== context.payload.workflow_run.run_attempt) return;
    // Only trust GitHub's metadata, never PR artifacts or code. Recheck the live
    // head so an old event cannot cancel the new revision's run.
    const prs = await github.paginate(github.rest.pulls.list, { ...repo, state: 'open', per_page: 100 });
    if (!prs.some(pr => pr.head.sha === run.head_sha && pr.head.ref === run.head_branch &&
        pr.head.repo?.full_name === run.head_repository?.full_name)) return;
    const jobs = await github.paginate(github.rest.actions.listJobsForWorkflowRunAttempt,
      { ...repo, run_id: run.id, attempt_number: run.run_attempt, per_page: 100 });
    if (await readDecision({ github, repo, run, jobs }) === 'deferred') {
      try {
        await github.rest.actions.cancelWorkflowRun({ ...repo, run_id: run.id });
      } catch (error) {
        if (error.status !== 409) throw error; // A job or another controller already canceled it.
      }
      return;
    }
    // Unrelated PR workflows need no admission monitoring. Empty step metadata
    // can mean a runner has not started yet, so allow the bounded retry below.
    if (jobs.some(job => job.steps?.length) &&
        !jobs.some(job => job.steps?.some(step => step.name === ADMISSION_STEP))) return;
    if (jobs.length && jobs.every(job => job.conclusion === 'skipped' || job.steps?.some(step =>
      step.name === ADMISSION_STEP && step.conclusion === 'success'))) return;
    await wait(3000);
  }
}

module.exports = { ADMISSION_STEP, marker, checkDeferredCI, jobDecision, readDecision, cancelFromJob, cancelDeferredRun };
