#!/usr/bin/env python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# This tool finds all package.json files and runs npm install and npm test in those directories.

import argparse
import os
import subprocess
from contextlib import contextmanager

@contextmanager
def pushd(path):
  currentDir = os.getcwd()
  os.chdir(path)
  yield
  os.chdir(currentDir)

def main():
  parser = argparse.ArgumentParser(description='Runs all Node.js tests')
  parser.add_argument('--force', action='store_true', help='Force updating @attic/noms')
  args = parser.parse_args()

  lsfiles = subprocess.check_output(['git', 'ls-files']).split('\n')
  lsfiles.sort(key = len) # Sort by shortest first to make sure we deal with parents first
  for f in lsfiles:
    path, name = os.path.split(f)
    if name == 'package.json':
      with pushd(path):
        subprocess.check_call(['npm', 'install'])
        if args.force and not path.endswith('js/noms'):
            subprocess.check_call(['npm', 'install', '@attic/noms'])
        subprocess.check_call(['npm', 'test'])

if __name__ == '__main__':
  main()
