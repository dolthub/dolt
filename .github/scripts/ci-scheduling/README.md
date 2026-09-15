# PR CI deferral

Apply either or both of these labels to control when automatic PR CI may start:

| Label | Condition for starting CI |
| --- | --- |
| `defer-ci-after-hours` | Wait during **5am–9pm**; release during **9pm–5am**. |
| `defer-ci-review` | Wait until the PR has at least one active approving review. |
| Both labels | Wait until **both** the after-hours window and an approval are present. |

All times use **America/Los_Angeles** by default. Set the repository Actions
variable `CI_TIMEZONE` to another IANA timezone name if needed. The release window
includes 9pm and ends at 5am, every day including weekends, with daylight-saving
time handled automatically.

For example, a PR with both labels that receives approval at 2pm waits until the
9pm release window. If it is still unapproved at 9pm, it continues waiting for an
approval. A PR with only `defer-ci-review` can start CI on approval at any time.

An active approval means a reviewer's latest submitted decision is `APPROVED`.
A later comment-only review does not revoke it; a dismissal or later request for
changes from that reviewer does. One reviewer's active approval is sufficient,
even if another reviewer requested changes. This is a CI scheduling condition;
it does not replace branch protection's review requirements. GitHub controls
whether new commits dismiss existing approvals. An approval that GitHub has not
dismissed continues to qualify, even if it was submitted on an earlier commit.

For a draft PR, add **`force-draft-ci`** to bypass the draft hold without marking
it ready. This label bypasses only the draft hold: `defer-ci-after-hours` and
`defer-ci-review` still apply, including when combined.

## Submitting a PR without starting CI before labels are applied

**Automatic PR CI is also held while a PR is a draft without `force-draft-ci`.**
Agents should create the PR as a draft, apply all desired labels, and then mark it
ready for review.
Marking it ready releases CI only when all label conditions are satisfied.
Unlabeled drafts also wait until they are marked ready or given `force-draft-ci`.

`gh pr create --label` is **not atomic**: the CLI creates the PR, then applies
labels in a separate API call. A quick label update alone cannot guarantee that
CI will not start first. GitHub's create-PR API does support creating a draft in
the initial request, which makes this sequence safe for automatic PR CI:

```sh
pr_url=$(gh pr create --draft \
  --label defer-ci-after-hours \
  --label defer-ci-review \
  --title 'PR title' --body-file pr-body.txt) &&
  gh pr ready "$pr_url"
```

To keep the PR in draft, replace the final `gh pr ready "$pr_url"` command with
`gh pr edit "$pr_url" --add-label force-draft-ci`. Apply the deferral labels
successfully **before** adding this override, so the draft hold protects the
label-assignment interval.

Use either deferral label or both as needed. The `&&` ensures the PR is not marked
ready if creation or label assignment fails; the draft continues to hold CI. The
repository labels must exist before running these commands. The gate reads the
live draft state and labels, including when an original draft run is rerun.

Sources: [GitHub CLI creation and metadata calls](https://github.com/cli/cli/blob/trunk/api/queries_pr.go#L459-L520),
[GitHub create-PR inputs](https://docs.github.com/en/graphql/reference/pulls#createpullrequestinput).

## Release and override

- Only small admission, scheduling, and review-notification jobs run while CI is
  deferred. Test jobs and their OS matrices do not acquire runners while waiting.
- One bot comment explains the outstanding conditions and the exact label changes
  that override each applicable hold. Release and error updates retain these
  instructions, including that each override clears only its own condition.
  The separate `ci-deferred` label tracks postponed work until its CI finishes.
  The scheduler manages this queue label; users choose the two `defer-ci-*` labels.
- Label additions/removals and draft-ready transitions trigger reconciliation.
  An approval triggers a small read-only review-notification workflow; its
  completion wakes the trusted controller, which fetches the current reviews.
  Review edits and dismissals also trigger that recheck.
- `Schedule PR CI` polls every 15 minutes, at :07, :22, :37, and :52. Review-only
  PRs, drafts, and tracked PRs whose deferral labels were removed are checked
  during daytime too. After-hours PRs are polled during the release window.
  Polling recovers missed events; GitHub schedules can be delayed or dropped.
- Remove a deferral label to remove that condition. Removing just one label does
  not bypass the other. Mark the PR ready (or add `force-draft-ci`) and remove
  both deferral labels to request ordinary CI immediately, without a new commit.
- For a manual retry, use Actions → **Schedule PR CI** → **Run workflow**, choosing
  the default branch and entering the PR number, or use **Re-run all jobs** on an
  original deferred run. Both recheck the live draft state, labels, and reviews;
  a manual retry does not bypass an outstanding condition.
- Labels apply to subsequent commits too. The labels stay on the PR after CI runs.

These conditions control admission of new work. Admitted work can continue after
5am; adding a label, dismissing a review, or converting the PR back to draft does
not cancel running tests. GitHub may delay assigning a runner after admission.
Existing non-PR triggers (manual branch runs, comment commands, pushes, repository
dispatches, releases, and nightly workflows) retain their previous behavior.

## Repository setup

1. Merge the workflows and scripts into the default branch. Scheduled and
   `workflow_run` triggers require that. The privileged controller explicitly
   checks out that branch, so full end-to-end validation requires deployment there.
   PR branches must contain the admission workflow and scripts, directly or
   through their PR merge commit.
2. **Require the `CI scheduling` status check** in branch protection or the ruleset
   for PR target branches, while keeping existing required test checks. Skipped
   Actions jobs count as successful; this pending status prevents merging deferred
   CI. Arrange this protection before using deferral. Source changes do not
   modify repository rules.
3. Create the three user-facing labels if they do not already exist:

   ```sh
   gh label create defer-ci-after-hours --repo dolthub/dolt \
     --description 'Defer PR CI until 9pm–5am' --color d4c5f9
   gh label create defer-ci-review --repo dolthub/dolt \
     --description 'Defer PR CI until an approving review' --color c5def5
   gh label create force-draft-ci --repo dolthub/dolt \
     --description 'Allow draft CI subject to deferral labels' --color fef2c0
   ```

   The controller creates the internal `ci-deferred` queue label automatically.
4. Optionally set `CI_TIMEZONE`. An invalid timezone fails after-hours admission.
5. The workflows use `GITHUB_TOKEN`; no new PAT or secret is needed. Only the
   trusted controller has `actions: write`, `statuses: write`, and `issues: write`.
   Review notifications have no repository permissions and do not check out code,
   so fork review events never execute PR code with the controller's write token.
6. Validate a draft-to-ready submission with each label and both labels. Verify
   daytime deferral, overnight release, approval/dismissal handling, label removal,
   the draft override, and fork PRs. GitHub's normal fork approval requirements
   remain in force; polling is a fallback if a review notification awaits approval.

The scheduling status remains pending until all participating workflows finish
without deferred or failed admission. Individual CI checks still determine test
results and mergeability, preserving which test suites are required or optional.
Admission errors fail the scheduling status. Failed tests are not automatically
retried; use their original workflow controls.

## Implementation and limits

- Every job in a PR workflow depends on the reusable `ci-admission.yaml`. Existing
  job conditions, dependencies, PR event types, and path filters are preserved.
- A deferred gate records a successful step named `CI postponed`; test jobs are
  skipped. The controller reads that step through the Actions API.
- The scheduler reruns the original workflows, preserving PR checks, checkout and
  merge refs, event payloads, concurrency groups, and original actor permissions.
- Only the latest run per workflow for the current open PR head is eligible.
  Before each rerun the controller rechecks the head, draft state, labels, review
  decisions, time, run state, and attempt. It does not release obsolete commits.
- A per-PR concurrency group serializes event and timer reconciliation. Polling
  recovers invocations replaced in GitHub's single pending concurrency slot.
- The bot comment caches completed admission decisions by commit, run, and attempt
  to avoid repeatedly fetching jobs. Live labels and reviews are never cached.
  Only the bot's own comment is read; a missing or damaged cache is rebuilt.
- The trusted controller never executes PR code or downloads PR artifacts.
- Register new PR workflows in `workflows.json` and the scheduler's subscriptions,
  and gate every job. A structural test checks that coverage. The review notifier
  is a metadata workflow and is not itself part of deferred CI.

GitHub permits reruns for **30 days after the original run**, up to **50 attempts**.
This also limits how long draft/review deferrals can be automatically released
using the original runs. If a review takes longer, or the rerun limit is exhausted,
the controller leaves CI pending and comments with recovery instructions: push a
new commit to create fresh PR runs. Two PR reconciliations may run concurrently
per scheduler invocation; this does not reserve organization-wide runner capacity.

## Tests

From the repository root:

```sh
node --test .github/scripts/ci-scheduling/*.test.js
actionlint -shellcheck='' .github/workflows/ci-admission.yaml \
  .github/workflows/ci-scheduler.yaml .github/workflows/ci-review-notification.yaml \
  .github/workflows/ci-scheduling-tests.yaml
```

The mocked API tests cover label combinations, active approvals and dismissal,
draft submission and its override, time boundaries and DST, comments, queue
recovery, stale/fork heads, retries, errors, and workflow wiring. They do not launch
actual CI or change repository settings.
