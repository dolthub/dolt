# Non-urgent PR CI

Put the literal, case-sensitive marker `[non-urgent]` anywhere in a PR description
to postpone its automatic PR CI during the day and release it overnight:

- **Deferral period: 5am–9pm.** New CI work waits until that evening's release window.
- **Release window: 9pm–5am.** Postponed CI is released automatically, and new CI
  work can start normally. The window includes 9pm and ends at 5am.

For example, CI for a marked PR opened at 2pm waits until the release window opens
at 9pm. CI for a marked PR opened at 11pm can start immediately.

All times use **America/Los_Angeles** by default. Configure the repository Actions
variable `CI_TIMEZONE` with an IANA timezone name to change the timezone.
Daylight-saving transitions are handled automatically. This schedule applies
every day, including weekends.

## Release and override

- New marked PRs and new commits run only the small admission and scheduling jobs
  during the 5am–9pm deferral period. The test jobs and their OS matrices do not
  acquire runners.
- A single bot comment explains the delay and is updated as work is released.
  The `ci-deferred` label tracks deferred work until its current CI finishes.
- `Schedule PR CI` checks every 15 minutes, at :07, :22, :37, and :52. During the
  9pm–5am release window, it scans open marked PRs as well as labeled PRs and
  releases postponed workflows, recovering missed initial events.
  GitHub schedules can be delayed or dropped; the next invocation retries.
- Remove the marker to release deferred workflows immediately through the PR
  description `edited` event. No new commit is necessary. A polling fallback also
  recovers missed removal events for tracked PRs during the deferral period.
- For an explicit manual retry, remove the marker and use Actions → **Schedule PR
  CI** → **Run workflow**, choosing the default branch and entering the PR number.
  Alternatively, use **Re-run all jobs** on an original deferred CI run. The gate
  fetches the live description on every attempt; the old event body is not used.
- A PR that keeps the marker also defers CI for subsequent commits made during
  5am–9pm until the next release window.
  Removing it restores ordinary CI for subsequent commits.

The 9pm–5am release window controls when CI is allowed to start. Work already
admitted can continue after 5am; it is not stopped when the deferral period begins.
Adding the marker does not cancel running tests. GitHub may delay assigning a
runner after admission. Existing non-PR triggers (manual branch runs, comment
commands, pushes, repository dispatches, releases, and nightly workflows) retain
their previous behavior.

## Repository setup

1. Merge these workflows and scripts into the default branch. GitHub's scheduled
   and `workflow_run` triggers require that. The privileged controller explicitly
   checks out the default branch, so it cannot be exercised end-to-end from an
   unmerged feature branch. PR branches must contain the new admission workflow
   and scripts, either directly or through their PR merge commit.
2. **Add `CI scheduling` as a required status check** in the branch protection rule
   or ruleset for PR target branches. Keep the existing required test checks too.
   A skipped Actions job counts as successful; the pending scheduling status is
   what prevents merging deferred CI. Arrange this protection as part of rollout,
   before using the marker. The source change does not modify repository rules.
3. Optionally set `CI_TIMEZONE`. An invalid timezone fails admission rather than
   allowing the expensive jobs to run.
4. The workflows use `GITHUB_TOKEN`, with `actions: write`, `statuses: write`, and
   `issues: write` only on the trusted controller. No new PAT or secret is needed.
   The controller creates `ci-deferred` automatically if it does not exist.
5. Verify using a small marked PR during the 5am–9pm deferral period: test jobs
   should be skipped, the comment and pending status should appear, and removing
   the marker should restart the original runs. Also verify overnight release and fork PRs in the
   live repository. Fork approval requirements remain in force.

The scheduling status stays pending while deferred runs are being released or
running. Once all participating workflows finish without a deferred or failed
admission, it succeeds: the original individual CI checks determine whether tests
passed and whether the PR can merge. This preserves which test suites are
required versus optional. Admission errors fail the scheduling status. Failed
tests are not automatically retried; use their original workflow controls.

## Implementation

- Every job in a PR workflow depends on the reusable `ci-admission.yaml` workflow.
  The job condition is evaluated before its test runners are allocated. Existing
  job conditions, dependencies, PR event types, and path filters are preserved.
- A deferred admission records a successful step named `CI postponed`; its test
  jobs are skipped. The controller reads that step from the current workflow
  attempt through the Actions API.
- The scheduler uses the **rerun API**, instead of creating branch dispatches.
  This preserves the original PR check associations, checkout/merge ref, event
  payload, concurrency groups, and the original actor's permission restrictions.
- Reruns are limited to the latest run per workflow for the current open PR head,
  matched by PR number, repository, and branch. Before each rerun the controller
  rechecks the live PR, time, run state, and attempt number. It never releases
  obsolete commits or automatically retries real test failures.
- A per-PR concurrency group serializes edits, completions, and timer invocations.
  GitHub can replace an older pending invocation; subsequent events and polling
  reconcile live state rather than relying on delivery of every event.
- The bot comment caches admission decisions by commit, run ID, and attempt to
  avoid repeatedly fetching all jobs. Only the bot's own comment is accepted.
  Deleting the comment or damaging the cache causes it to be rebuilt from the API.
- The controller never checks out PR code, runs PR scripts, or downloads PR
  artifacts with its write token. PR admission jobs have read-only permissions.
- New PR workflows must be added to `workflows.json` and the scheduler's
  `workflow_run.workflows` subscription, with all their jobs gated. A structural
  test checks this coverage.

GitHub limits reruns to 30 days after the original run and 50 attempts. Normal
next-night deferrals are well within those limits. If a long outage or repeated
manual reruns exhausts them, the controller leaves CI pending and updates the
comment with recovery instructions; push a new commit to create fresh PR runs.
Two PR reconciliations can run concurrently per scheduler invocation. This does
not reserve runner capacity or provide an organization-wide urgent-work queue.

## Tests

From the repository root:

```sh
node --test .github/scripts/ci-scheduling/*.test.js
actionlint -shellcheck='' .github/workflows/ci-admission.yaml \
  .github/workflows/ci-scheduler.yaml .github/workflows/ci-scheduling-tests.yaml
```

The Node tests use a mocked GitHub API and cover admission boundaries and DST,
live marker removal, pending statuses, queue recovery, comments, stale and fork
heads, retries, failures, and workflow wiring. They do not launch actual CI or
change repository settings.
