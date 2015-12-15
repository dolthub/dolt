#!/usr/bin/python

import argparse, os.path

def Main(projectName, stagingFunction):
	"""Main should be called by all staging scripts when executed.

	Main takes a project name and a callable. It creates a staging
	directory for your project and then runs the callable, passing it the
	path to the newly-created staging directory.
	Example:

	#!/usr/bin/python
	import shutil

	import noms.staging

	def stage(stagingDir):
		shutil.copy2('out.js', stagingDir)
		shutil.copy2('index.html', stagingDir)
		shutil.copy2('styles.css', stagingDir)


	if __name__ == '__main__':
		noms.staging.Main('nerdosphere', stage)
	"""
	parser = argparse.ArgumentParser(description='Stage build products from this directory.')
	parser.add_argument('stagingDir', metavar='path/to/staging/directory', type=_dirPath, help='top-level dir into which project build products are staged')
	args = parser.parse_args()
	projectStagingDir = os.path.join(args.stagingDir, projectName)

	normalized = os.path.realpath(projectStagingDir)
	if not _isSubDir(projectStagingDir, args.stagingDir):
		raise Exception(projectStagingDir + ' must be a subdir of ' + args.stagingDir)

	os.makedirs(normalized)
	stagingFunction(normalized)


def _dirPath(arg):
	normalized = os.path.realpath(os.path.abspath(arg))
	if not os.path.isdir(normalized):
		raise ValueError(arg + ' is not a path to a directory.')
	return normalized


def _isSubDir(subdir, directory):
    # Need the path-sep at the end to ensure that commonprefix returns the correct result below.
    directory = os.path.join(os.path.realpath(directory), '')
    subdir = os.path.realpath(subdir)

    # return true, if the common prefix of both is equal to directory e.g. /a/b/c/d.rst and directory is /a/b, the common prefix is /a/b
    return os.path.commonprefix([subdir, directory]) == directory