#!/usr/bin/python

import os, os.path, subprocess, sys

sys.path.append(os.path.expandvars('$GOPATH/src/github.com/attic-labs/noms/tools'))

import noms.symlink as symlink

def main():
	# ln -sf ../../js/.babelrc .babelrc hack, because zip files screw up symlinks.
	babelrcPath = os.path.abspath('.babelrc')
	symlink.Force('../../../js/.babelrc', babelrcPath)

	subprocess.check_call('./link.sh', shell=False)
	subprocess.check_call(['npm', 'install'], shell=False)
	env = os.environ
	if 'NOMS_SERVER' not in env:
		env['NOMS_SERVER'] = 'http://localhost:8000'
	if 'NOMS_DATASET_ID' not in env:
		env['NOMS_DATASET_ID'] = 'mlb/heatmap'
	subprocess.check_call(['npm', 'run', 'build'], env=env, shell=False)


if __name__ == "__main__":
	main()