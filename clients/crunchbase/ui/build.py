#!/usr/bin/python

import os, os.path, subprocess, sys

sys.path.append(os.path.abspath('../../../tools'))

import noms.symlink as symlink

def main():
    symlink.Force('../../../js/.babelrc', os.path.abspath('.babelrc'))
    symlink.Force('../../../js/.flowconfig', os.path.abspath('.flowconfig'))

    subprocess.check_call(['npm', 'install'], shell=False)
    env = {
        'NOMS_SERVER': 'http://localhost:8000',
        'NOMS_DATASET_ID': 'crunchbase/index',
    }.update(os.environ)
    subprocess.check_call(['npm', 'run', 'build'], env=env, shell=False)


if __name__ == "__main__":
    main()
