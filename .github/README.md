# Dolt's GitHub Actions

This doc will provide context for the types of Workflows we use in this repository. This doc is not a comprehensive GitHub Actions tutorial. To familiarize yourself with GitHub Actions concepts and the terminology, please see the [documentation](https://docs.github.com/en/actions/learn-github-actions/understanding-github-actions).

Dolt uses GitHub Actions Workflows in four primary ways:

 * To run continuous integration tests on pull requests and pushes to `main`
 * To release and publish new Dolt assets
 * To deploy various benchmarking jobs to contexts _other_ than GitHub Actions (like in a Kubernetes cluster, for example).
 * To handle misc. [repository_dispatch](https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows#repository_dispatch) events triggered by external clients.

 ## Continuous Integration Workflows

Workflows prefixed with `ci-` are run on pull requests to `main`, though some run on pushes to `main` (after a pull request is merged). These workflows are synchronous and don't trigger any other workflows to run.

## Dolt Release Workflows

Workflows prefixed with `cd-` are used for releasing Dolt. Some of these workflows are asynchronous, meaning that they only perform part of a task before triggering the next part of a task to run in a _different_ workflow, sometimes in other GitHub repositories, using `repository_dispatch` events.

## Benchmarking Workflows

Benchmarking workflows are used as an interface for deploying benchmarking jobs to one of our Kubernetes Clusters. Workflows that deploy Kubernetes Jobs are prefixed with `k8s-` and can only be triggered with `repository_dispatch` events. Notice that benchmarking workflows, like `workflows/performance-benchmarks-email-report.yaml` for example, trigger these events using the `peter-evans/repository-dispatch@v1` Action.

These Kubernetes Jobs do not run on GitHub Actions Hosted Runners, so the workflow logs do not contain any information about the deployed Kubernetes Job or any errors it might have encountered. The workflow logs can only tell you if a Job was created successfully or not. To investigate an error or issue with a Job in our Kubernetes Cluster, see the debugging guide [here](https://github.com/dolthub/ld/blob/main/k8s/README.md#debug-performance-benchmarks-and-sql-correctness-jobs).

## Misc. Repository Dispatch Workflows

Some workflows perform single, common tasks and are triggered by `repository_dispatch` events. These include the `workflows/email-report.yaml` that emails the results of performance benchmarks to the team, or the `workflows/pull-report.yaml` that posts those same results to an open pull request. Workflows like these are triggered by external clients.
