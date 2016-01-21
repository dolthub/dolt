#!/usr/bin/python

import os, subprocess, sys, shutil

SRC = ['babel-regenerator-runtime', 'src/main.js']
OUT = 'out.js'

def main():
	env = os.environ
	env['NODE_ENV'] = sys.argv[1]
	env['BABEL_ENV'] = sys.argv[1]
	if 'NOMS_SERVER' not in env:
		env['NOMS_SERVER'] = 'http://localhost:8000'
	if 'NOMS_DATASET_ID' not in env:
		env['NOMS_DATASET_ID'] = 'crunchbase/index'

	if sys.argv[1] == 'production':
		shutil.copyfile('node_modules/nvd3/build/nv.d3.min.css', 'nvd3.css')
		shutil.copyfile('node_modules/nvd3/build/nv.d3.min.js', 'nvd3.js')
		shutil.copyfile('node_modules/d3/d3.min.js', 'd3.js')
	else:
		shutil.copyfile('node_modules/nvd3/build/nv.d3.css', 'nvd3.css')
		shutil.copyfile('node_modules/nvd3/build/nv.d3.js', 'nvd3.js')
		shutil.copyfile('node_modules/d3/d3.js', 'd3.js')

	subprocess.check_call(['node_modules/.bin/webpack'] + SRC + [OUT], env=env, shell=False)


if __name__ == "__main__":
	main()
