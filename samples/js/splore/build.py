#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import subprocess

def main():
    subprocess.check_call(['yarn'], shell=False)

if __name__ == "__main__":
    main()
