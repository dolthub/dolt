#!/usr/bin/python

# Copyright 2016 The Noms Authors. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os, os.path, subprocess, sys

sys.path.append(os.path.abspath('../../../tools'))

import noms.symlink as symlink

def main():
    symlink.Force('../../../js/.babelrc', os.path.abspath('.babelrc'))
    symlink.Force('../../../js/.flowconfig', os.path.abspath('.flowconfig'))

    subprocess.check_call(['npm', 'install'], shell=False)
    subprocess.check_call(['npm', 'run', 'build'], env=os.environ, shell=False)


if __name__ == "__main__":
    main()
