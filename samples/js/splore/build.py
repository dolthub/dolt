#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os, os.path, subprocess, sys
from contextlib import contextmanager

sys.path.append(os.path.abspath('../../../tools'))

import noms.symlink as symlink

@contextmanager
def pushd(path):
    currentDir = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(currentDir)

def main():
    with pushd('../../../js/noms'):
        subprocess.check_call(['npm', 'install'], shell=False)

    with pushd('../'):
        subprocess.check_call(['npm', 'install'], shell=False)

    subprocess.check_call(['npm', 'install'], shell=False)

if __name__ == "__main__":
    main()
