#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os
from contextlib import contextmanager

@contextmanager
def pushd(path):
    currentDir = os.getcwd()
    os.chdir(path)
    yield
    os.chdir(currentDir)
