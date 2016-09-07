#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os
import sys

sys.path.append(os.path.abspath('../../../tools'))
import noms.staging as staging

if __name__ == '__main__':
    staging.Main('perf', staging.GlobCopier(
        'main.css',
        'modules/Chart.bundle.min.js',
        'out.js',
        index_file='index.html',
        rename=True
    ))
