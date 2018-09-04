# Contributing

1. Sign one of the contributor license agreements below.
1. `go get golang.org/x/review/git-codereview` to install the code reviewing tool.
    1. You will need to ensure that your `GOBIN` directory (by default
       `$GOPATH/bin`) is in your `PATH` so that git can find the command.
    1. If you would like, you may want to set up aliases for git-codereview,
       such that `git codereview change` becomes `git change`. See the
       [godoc](https://godoc.org/golang.org/x/review/git-codereview) for details.
    1. Should you run into issues with the git-codereview tool, please note
       that all error messages will assume that you have set up these
       aliases.
1. Get the cloud package by running `go get -d cloud.google.com/go`.
    1. If you have already checked out the source, make sure that the remote git
       origin is https://code.googlesource.com/gocloud:

            git remote set-url origin https://code.googlesource.com/gocloud
1. Make sure your auth is configured correctly by visiting
   https://code.googlesource.com, clicking "Generate Password", and following
   the directions.
1. Make changes and create a change by running `git codereview change <name>`,
provide a commit message, and use `git codereview mail` to create a Gerrit CL.
1. Keep amending to the change with `git codereview change` and mail as your receive
feedback. Each new mailed amendment will create a new patch set for your change in Gerrit.

## Integration Tests

In addition to the unit tests, you may run the integration test suite.

To run the integrations tests, creating and configuration of a project in the
Google Developers Console is required.

After creating a project, you must [create a service account](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount).
Ensure the project-level **Owner**
[IAM role](console.cloud.google.com/iam-admin/iam/project) role is added to the
service account. Alternatively, the account can be granted all of the following roles:
- **Editor**
- **Logs Configuration Writer**
- **PubSub Admin**

Once you create a project, set the following environment variables to be able to
run the against the actual APIs.

- **GCLOUD_TESTS_GOLANG_PROJECT_ID**: Developers Console project's ID (e.g. bamboo-shift-455)
- **GCLOUD_TESTS_GOLANG_KEY**: The path to the JSON key file.

Some packages require additional environment variables to be set:

- firestore
  - **GCLOUD_TESTS_GOLANG_FIRESTORE_PROJECT_ID**: project ID for Firestore.
  - **GCLOUD_TESTS_GOLANG_FIRESTORE_KEY**: The path to the JSON key file.
- storage
  - **GCLOUD_TESTS_GOLANG_KEYRING**: The full name of the keyring for the tests, in the
    form "projects/P/locations/L/keyRings/R".
- translate
  - **GCLOUD_TESTS_API_KEY**: API key for using the Translate API.
- profiler
  - **GCLOUD_TESTS_GOLANG_ZONE**: Compute Engine zone.

Some packages can record the RPCs during integration tests to a file for
subsequent replay. To record, pass the `-record` flag to `go test`. The
recording will be saved to the _package_`.replay` file. To replay integration
tests from a saved recording, the replay file must be present, the `-short` flag
must be passed to `go test`, and the **GCLOUD_TESTS_GOLANG_ENABLE_REPLAY**
environment variable must have a non-empty value.

Install the [gcloud command-line tool][gcloudcli] to your machine and use it
to create some resources used in integration tests.

From the project's root directory:

``` sh
# Set the default project in your env.
$ gcloud config set project $GCLOUD_TESTS_GOLANG_PROJECT_ID

# Authenticate the gcloud tool with your account.
$ gcloud auth login

# Create the indexes used in the datastore integration tests.
$ gcloud preview datastore create-indexes datastore/testdata/index.yaml

# Create a Google Cloud storage bucket with the same name as your test project,
# and with the Stackdriver Logging service account as owner, for the sink
# integration tests in logging.
$ gsutil mb gs://$GCLOUD_TESTS_GOLANG_PROJECT_ID
$ gsutil acl ch -g cloud-logs@google.com:O gs://$GCLOUD_TESTS_GOLANG_PROJECT_ID

# Create a PubSub topic for integration tests of storage notifications.
$ gcloud beta pubsub topics create go-storage-notification-test

# Create a Spanner instance for the spanner integration tests.
$ gcloud beta spanner instances create go-integration-test --config regional-us-central1 --nodes 1 --description 'Instance for go client test'
# NOTE: Spanner instances are priced by the node-hour, so you may want to delete
# the instance after testing with 'gcloud beta spanner instances delete'.

# For Storage integration tests:
# Enable KMS for your project in the Cloud Console.
# Create a KMS keyring, in the same location as the default location for your project's buckets.
$ gcloud kms keyrings create MY_KEYRING --location MY_LOCATION
# Create two keys in the keyring, named key1 and key2.
$ gcloud kms keys create key1 --keyring MY_KEYRING --location MY_LOCATION --purpose encryption
$ gcloud kms keys create key2 --keyring MY_KEYRING --location MY_LOCATION --purpose encryption
# As mentioned above, set the GCLOUD_TESTS_GOLANG_KEYRING environment variable.
$ export GCLOUD_TESTS_GOLANG_KEYRING=projects/$GCLOUD_TESTS_GOLANG_PROJECT_ID/locations/MY_LOCATION/keyRings/MY_KEYRING
# Authorize Google Cloud Storage to encrypt and decrypt using key1.
gsutil kms authorize -p $GCLOUD_TESTS_GOLANG_PROJECT_ID -k $GCLOUD_TESTS_GOLANG_KEYRING/cryptoKeys/key1
```

Once you've done the necessary setup, you can run the integration tests by running:

``` sh
$ go test -v cloud.google.com/go/...
```

## Contributor License Agreements

Before we can accept your pull requests you'll need to sign a Contributor
License Agreement (CLA):

- **If you are an individual writing original source code** and **you own the
intellectual property**, then you'll need to sign an [individual CLA][indvcla].
- **If you work for a company that wants to allow you to contribute your
work**, then you'll need to sign a [corporate CLA][corpcla].

You can sign these electronically (just scroll to the bottom). After that,
we'll be able to accept your pull requests.

## Contributor Code of Conduct

As contributors and maintainers of this project,
and in the interest of fostering an open and welcoming community,
we pledge to respect all people who contribute through reporting issues,
posting feature requests, updating documentation,
submitting pull requests or patches, and other activities.

We are committed to making participation in this project
a harassment-free experience for everyone,
regardless of level of experience, gender, gender identity and expression,
sexual orientation, disability, personal appearance,
body size, race, ethnicity, age, religion, or nationality.

Examples of unacceptable behavior by participants include:

* The use of sexualized language or imagery
* Personal attacks
* Trolling or insulting/derogatory comments
* Public or private harassment
* Publishing other's private information,
such as physical or electronic
addresses, without explicit permission
* Other unethical or unprofessional conduct.

Project maintainers have the right and responsibility to remove, edit, or reject
comments, commits, code, wiki edits, issues, and other contributions
that are not aligned to this Code of Conduct.
By adopting this Code of Conduct,
project maintainers commit themselves to fairly and consistently
applying these principles to every aspect of managing this project.
Project maintainers who do not follow or enforce the Code of Conduct
may be permanently removed from the project team.

This code of conduct applies both within project spaces and in public spaces
when an individual is representing the project or its community.

Instances of abusive, harassing, or otherwise unacceptable behavior
may be reported by opening an issue
or contacting one or more of the project maintainers.

This Code of Conduct is adapted from the [Contributor Covenant](http://contributor-covenant.org), version 1.2.0,
available at [http://contributor-covenant.org/version/1/2/0/](http://contributor-covenant.org/version/1/2/0/)

[gcloudcli]: https://developers.google.com/cloud/sdk/gcloud/
[indvcla]: https://developers.google.com/open-source/cla/individual
[corpcla]: https://developers.google.com/open-source/cla/corporate
