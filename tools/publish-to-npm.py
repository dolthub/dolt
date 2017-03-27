#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

from distutils.version import LooseVersion
import json
import os
import subprocess
from noms.pushd import pushd

packages = [
    'babel-preset-noms',
    'eslint-config-noms',
    'webpack-config',
]

def main():
    '''Publishes the latest version of the js npm packages.
    '''

    gopath = os.getenv('GOPATH')
    if gopath is None:
        raise 'GOPATH not found in environemnt'

    for p in packages:
        with pushd(os.path.join(gopath, 'src', 'github.com', 'attic-labs', 'noms', 'js', p)):
            npm_publish()

def npm_publish():
    with open('package.json') as pkg:
        data = json.load(pkg)
    package_name = data['name']
    deployed_version = LooseVersion(
        subprocess.check_output(['npm', 'info', package_name, 'version']).strip())
    new_version = LooseVersion(data['version'])
    print '%s: Old version: %s, New version: %s' % (package_name, deployed_version, new_version)
    if new_version > deployed_version:
        subprocess.check_call(['npm', 'whoami'])
        subprocess.check_call(['yarn'])
        subprocess.check_call(['npm', 'publish'])

if __name__ == '__main__':
    main()
