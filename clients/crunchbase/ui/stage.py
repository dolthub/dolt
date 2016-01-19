#!/usr/bin/python

import shutil

import noms.staging

def stage(stagingDir):
	shutil.copy2('out.js', stagingDir)
	shutil.copy2('index.html', stagingDir)
	shutil.copy2('styles.css', stagingDir)
	shutil.copy2('nvd3.css', stagingDir)
	shutil.copy2('d3.js', stagingDir)
	shutil.copy2('nvd3.js', stagingDir)
	shutil.copy2('cookie.png', stagingDir)


if __name__ == '__main__':
	noms.staging.Main('nerdosphere', stage)