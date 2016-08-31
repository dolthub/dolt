#!/usr/bin/env python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

'''
This script builds the JS SDK documentation and puts the generated files
in $WORKSPACE/build.
'''

import os, subprocess, json

def main():
    # Workspace is the root of the builder, e.g. "/var/lib/jenkins/workspace/builder".
    workspace = os.getenv('WORKSPACE')
    assert workspace

    noms_dir = os.path.join(workspace, 'src/github.com/attic-labs/noms')
    noms_js_dir = os.path.join(noms_dir, 'js/noms')
    with open(os.path.join(noms_js_dir, 'package.json')) as pkg:
        data = json.load(pkg)

    version = data['version']

    os.chdir(workspace)
    subprocess.check_call(['npm', 'install', 'documentation'])

    cmd = [
        os.path.join(workspace, 'node_modules', '.bin', 'documentation'), 'build',
        os.path.join(noms_js_dir, 'src', 'noms.js'),
        '--name', 'Noms',
        '--document-exported',
        '--infer-private', '^_',
        '--github',
        '--format', 'html',
        '--output', os.path.join(workspace, 'build', version),
        '--project-version', version
    ]
    subprocess.check_call(cmd)

if __name__ == '__main__':
    main()
