#!/usr/bin/env python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# This tool updates the npm version of @attic/noms, @attic/eslintrc, etc in all package.json files.
#
# Run this whenever the version of an npm package is changed, then for each,
#  1. Run "npm i".
#  2. Update the code.
#  3. Run "npm test". If it fails, goto 2.
#
# Of course, every checkout will still need to run "npm i" to pick up the changes.

import collections
import json
import os
import subprocess
import sys

packagepaths = {
  '@attic/noms': 'js/noms/package.json',
  '@attic/eslintc': 'js/eslintrc/package.json',
  '@attic/webpack-config': 'js/webpack-config/package.json',
}

packageversions = {}

def main():
  for name, path in packagepaths.iteritems():
    if not os.path.exists(path):
      print('%s not found. Are you running from the noms root directory?' % (path,))
      sys.exit(1)
    with open(path, 'r') as f:
      pathjson = json.load(f)
      packageversions[name] = pathjson['version']

  updatedclients = set()

  lsfiles = subprocess.check_output(['git', 'ls-files']).split('\n')
  for f in lsfiles:
    path, name = os.path.split(f)
    if name == 'package.json' and update(f):
      updatedclients.add(path)

  if len(updatedclients) > 0:
    print('\n%s clients were updated. Run "npm i" in these directories:' % len(updatedclients))
    for client in updatedclients:
      print(client)
  else:
    print('Already up to date.')

def update(path):
  with open(path, 'r') as f:
    pathjson = json.load(f, object_pairs_hook=collections.OrderedDict)

  didupdate = False

  for depkey in ('dependencies', 'devDependencies'):
    deps = pathjson.get(depkey)
    if deps is None:
      continue

    for name, newversion in packageversions.iteritems():
      curversion = deps.get(name)
      newversion = '^' + newversion
      if curversion is None or curversion == newversion:
        continue

      deps[name] = newversion
      didupdate = True
      print('%s %s updated from %s to %s' % (path, name, curversion, newversion))

  with open(path, 'w') as f:
    json.dump(pathjson, f, indent=2, separators=(',', ': '))
    f.write('\n')

  return didupdate


if __name__ == '__main__':
  main()
