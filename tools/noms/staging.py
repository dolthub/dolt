#!/usr/bin/python

import argparse, glob, shutil, os.path

def Main(projectName, stagingFunction):
	"""Main should be called by all staging scripts when executed.

	Main takes a project name and a callable. It creates a staging directory for
	your project and then runs the callable, passing it the path to the
	newly-created staging directory.

	For the common case of simply copying a set of files into the staging
	directory, use GlobCopier:

	#!/usr/bin/python

	import noms.staging as staging

	if __name__ == '__main__':
		staging.Main('nerdosphere', staging.GlobCopier('index.html', 'styles.css', '*.js'))
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


def GlobCopier(*globs):
	exclude = ('webpack.config.js',)
	def stage(stagingDir):
		for g in globs:
			for f in glob.glob(g):
				if os.path.split(f)[1] not in exclude:
					shutil.copy2(f, stagingDir)
	return stage


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
