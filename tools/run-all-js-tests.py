#!/usr/bin/env python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# This tool finds all package.json files and runs npm install and npm test in those directories.

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
  lsfiles = subprocess.check_output(['git', 'ls-files']).split('\n')
  lsfiles.sort(key = len) # Sort by shortest first to make sure we deal with parents first
  for f in lsfiles:
    path, name = os.path.split(f)
    if name == 'package.json':
      with pushd(path):
        subprocess.check_call(['npm', 'install'])
        subprocess.check_call(['npm', 'test'])

if __name__ == '__main__':
  main()
