#!/usr/bin/env python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

'''This script runs the Noms PR Jenkins jobs on http://jenkins-perf.noms.io:
- http://jenkins-perf.noms.io/job/NomsMasterPerf
- http://jenkins-perf.noms.io/job/NomsPRPerf
'''

import copy
import os
import os.path
import re
import subprocess

# These are the 'go test' packages for perf testing.
# Note that adding entires will actually run all tests in that package, not just the perf tests.
PACKAGES = [
    './go/types/perf',
    './samples/go/csv/csv-import',
    './samples/go/url-fetch/perf',
]

# 'go test' timeout. Go's default is 10m, which isn't long enough.
TIMEOUT = '30m'

# Number of perf test repetitions. 3 is a good sample size, any more will take too long.
PERF_REPEAT = '3'

def main():
    # Workspace is the root of the builder, e.g. "/var/lib/jenkins/workspace/NomsMasterPerf".
    workspace = os.getenv('WORKSPACE')
    assert workspace

    # Directory where Go binaries have been installed.
    go_bin = '/usr/local/go/bin'
    assert os.path.exists(go_bin)

    # Jenkins has arranged for the testdata directory to be in a shared workspace, as opposed to
    # noms/../testdata like a normal checkout.
    jenkins_home = os.getenv('JENKINS_HOME')
    assert jenkins_home
    testdata = os.path.join(jenkins_home, 'sharedspace/testdata/src/github.com/attic-labs/testdata')
    assert os.path.exists(testdata)

    # PRs have a "sha1" environment variable. This will actually look like "origin/pr/2393/merge",
    # so extract the PR number to use as a prefix.
    # For the master builder, just use the prefix "master".
    pr_branch = os.getenv('sha1')
    if pr_branch:
        pr_pattern = re.compile(r'^origin/pr/(\d+)/merge$')
        pr_groups = pr_pattern.match(pr_branch)
        assert pr_groups
        perf_prefix = 'pr_%s/' % (pr_groups.group(1),) # pr_2393/
    else:
        perf_prefix = 'master/'

    # The database access token is given in a NOMS_ACCESS_TOKEN environment variable, in an attempt
    # to hide it from the public Jenkins logs.
    access_token = os.getenv('NOMS_ACCESS_TOKEN')
    assert access_token

    # Run test packages individually so they don't interfere with each other.
    for package in PACKAGES:
        cmd = [os.path.join(go_bin, 'go'), 'test', '-timeout', TIMEOUT, package,
               '-perf', 'http://demo.noms.io/perf?access_token=%s' % (access_token,),
               '-perf.repeat', PERF_REPEAT,
               '-perf.prefix', perf_prefix,
               '-perf.testdata', testdata]
        cwd = os.path.join(workspace, 'src/github.com/attic-labs/noms')
        env = copy.copy(os.environ)
        env.update({
            'GOPATH': workspace,
            'PATH': '%s:%s' % (os.getenv('PATH'), go_bin),
        })

        proc = subprocess.Popen(cmd, cwd=cwd, env=env)
        proc.wait()
        assert proc.returncode == 0


if __name__ == '__main__':
    main()
