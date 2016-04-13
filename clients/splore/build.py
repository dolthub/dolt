#!/usr/bin/python

import os, os.path, subprocess, sys

sys.path.append(os.path.abspath('../../tools'))

import noms.symlink as symlink

def main():
    symlink.Force('../../js/.babelrc', os.path.abspath('.babelrc'))
    symlink.Force('../../js/.eslintrc', os.path.abspath('.eslintrc'))
    symlink.Force('../../js/.flowconfig', os.path.abspath('.flowconfig'))

    subprocess.check_call(['npm', 'install'], shell=False)
    # TODO: Publish noms-webpack-config to npm.
    subprocess.check_call(['npm', 'install', 'noms-webpack-config'], shell=False)
    subprocess.check_call(['npm', 'run', 'build'], env=os.environ, shell=False)


if __name__ == "__main__":
    main()
