#!/usr/bin/python

import os, os.path, shutil, tempfile, unittest
import staging

class TestStaging(unittest.TestCase):
	CONTENTS = 'test file contents'

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


if __name__ == '__main__':
    unittest.main()