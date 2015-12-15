#!/usr/bin/python

import os, os.path, shutil, tempfile, unittest
import copy

class TestCopy(unittest.TestCase):
	def setUp(self):
		self.tempdir = os.path.realpath(tempfile.mkdtemp())


	def tearDown(self):
		shutil.rmtree(self.tempdir, ignore_errors=True)


	def test_CopyPeers(self):
		nested = tempfile.mkdtemp(dir=self.tempdir)
		otherNested = tempfile.mkdtemp(dir=self.tempdir)

		def mkfile():
			ret = tempfile.NamedTemporaryFile(dir=nested, delete=False)
			ret.close()
			return ret.name

		me = mkfile()
		peerFile = os.path.basename(mkfile())
		peerDir = os.path.basename(tempfile.mkdtemp(dir=nested))
		peerLink = 'link'
		peerLinkAbs = os.path.join(nested, 'link')
		os.symlink(peerFile, peerLinkAbs)

		copy.Peers(me, otherNested)
		self.assertTrue(os.path.islink(os.path.join(otherNested, peerLink)))
		self.assertTrue(os.path.isfile(os.path.join(otherNested, peerFile)))
		self.assertTrue(os.path.isdir(os.path.join(otherNested, peerDir)))


if __name__ == '__main__':
    unittest.main()