#!/usr/bin/python

import os, subprocess, sys

SRC = ['babel-regenerator-runtime', 'src/main.js']
OUT = 'out.js'

def main():
	env = os.environ
	env['NODE_ENV'] = sys.argv[1]
	env['BABEL_ENV'] = sys.argv[1]
	if 'NOMS_SERVER' not in env:
		env['NOMS_SERVER'] = 'http://localhost:8000'
	if 'NOMS_DATASET_ID' not in env:
		env['NOMS_DATASET_ID'] = 'mlb/heatmap'
	subprocess.check_call(['node_modules/.bin/webpack'] + SRC + [OUT], env=env, shell=False)


if __name__ == "__main__":
	main()
