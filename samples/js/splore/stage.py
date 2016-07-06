#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import noms.staging as staging


if __name__ == '__main__':
	staging.Main('splore', staging.GlobCopier('out.js', 'index.html', 'styles.css'))
