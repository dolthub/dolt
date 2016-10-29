#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os, subprocess, json
from distutils.version import LooseVersion

def main():
  os.chdir(os.path.join('js', 'noms'))
  deployed_version = LooseVersion(
    subprocess.check_output(['npm', 'info', '@attic/noms', 'version']).strip())
  with open('package.json') as pkg:
    data = json.load(pkg)
  new_version = LooseVersion(data['version'])
  print 'Old version: %s, New version: %s' % (deployed_version, new_version)
  if new_version > deployed_version:
    subprocess.check_call(['npm', 'whoami'])
    subprocess.check_call(['npm', 'install'])
    subprocess.check_call(['npm', 'publish'])

if __name__ == '__main__':
  main()
