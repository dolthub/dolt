#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os, os.path, shutil, tempfile, unittest
import staging

class TestStaging(unittest.TestCase):
	def setUp(self):
		self.tempdir = os.path.realpath(tempfile.mkdtemp())
		self.nested = tempfile.mkdtemp(dir=self.tempdir)

	def tearDown(self):
		shutil.rmtree(self.tempdir, ignore_errors=True)

	def test_Nested(self):
		self.assertTrue(staging._isSubDir(self.nested, self.tempdir))

	def test_NotNested(self):
		otherNested = tempfile.mkdtemp(dir=self.tempdir)
		self.assertFalse(staging._isSubDir(self.nested, otherNested))

	def test_DotDotNotReallyNested(self):
		notReallyNested = os.path.join(self.tempdir, 'foo', os.pardir, 'bar')
		self.assertFalse(staging._isSubDir(self.nested, notReallyNested))

	def test_LinkNotReallyNested(self):
		otherNested = tempfile.mkdtemp(dir=self.tempdir)
		linkName = os.path.join(self.nested, 'link')
		os.symlink(otherNested, linkName)
		self.assertFalse(staging._isSubDir(linkName, self.nested))

	def test_DirPath(self):
		linkName = os.path.join(self.tempdir, 'link')
		os.symlink(self.nested, linkName)
		norm = staging._dirPath(linkName)
		self.assertEqual(self.nested, norm)

	def test_DirPathFails(self):
		f = tempfile.NamedTemporaryFile(dir=self.tempdir)
		try:
			staging._dirPath(f.name)
		except ValueError:
			pass

	def test_GlobCopier(self):
		files = (
				'a.js',
				'b.js',
				'c.html',
				'd.css',
				'webpack.config.js',

				'x/aa.js',
				'x/bb.js',
				'x/dd.css',
				'x/webpack.config.js',

				'x/xx/aaa.js',
				'x/xx/bbb.js',
				'x/xx/webpack.config.js',

				'x/yy/aaa.js',
				'x/yy/bbb.js',
				'x/yy/webpack.config.js',

				'y/aaaa.js',
				'y/bbbb.js',
				'y/webpack.config.js',

				'y/xxx/a5.js',
				'y/xxx/b5.js',
				'y/xxx/webpack.config.js',

				'z/a6.js',
				'z/b6.js',
				'z/webpack.config.js',
				)
		for d in ('x/xx', 'x/yy', 'y/xxx', 'z'):
			os.makedirs(os.path.join(self.tempdir, d))
		for name in files:
			with open(os.path.join(self.tempdir, name), 'w') as f:
				f.write('hi')

		cwd = os.getcwd()
		try:
			os.chdir(self.tempdir)
			staging.GlobCopier('*.js', 'c.html', 'x/*.js', 'x/xx/*', 'y/*', 'y/*')(self.nested)
		finally:
			os.chdir(cwd)

		self.assertEqual(['a.js', 'b.js', 'c.html', 'x', 'y'], os.listdir(self.nested))
		self.assertEqual(['aa.js', 'bb.js', 'xx'], os.listdir(os.path.join(self.nested, 'x')))
		self.assertEqual(['aaa.js', 'bbb.js'], os.listdir(os.path.join(self.nested, 'x/xx')))
		self.assertEqual(['aaaa.js', 'bbbb.js'], os.listdir(os.path.join(self.nested, 'y')))


if __name__ == '__main__':
    unittest.main()
