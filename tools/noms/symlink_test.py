#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os, os.path, shutil, tempfile, unittest
import symlink

class TestForceSymlink(unittest.TestCase):
    CONTENTS = 'test file contents'

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.source = tempfile.NamedTemporaryFile(dir=self.tempdir, delete=False)
        with self.source.file as f:
            f.write(self.CONTENTS)


    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)


    def verifySymlink(self, linkName):
        with open(linkName, 'r') as f:
            actual = f.read()
            self.assertEqual(self.CONTENTS, actual)


    def test_ClobberFile(self):
        linkName = os.path.join(self.tempdir, 'link')
        with open(linkName, 'w') as f:
            f.write('foo')

        symlink.Force(self.source.name, linkName)
        self.verifySymlink(linkName)


    def test_ClobberSymlink(self):
        linkName = os.path.join(self.tempdir, 'link')
        os.symlink('nowhere', linkName)

        symlink.Force(self.source.name, linkName)
        self.verifySymlink(linkName)


    def test_NoClobberDir(self):
        linkName = os.path.join(self.tempdir, 'link')
        os.mkdir(linkName, 0777)

        try:
            symlink.Force(self.source.name, linkName)
        except symlink.LinkError:
            pass


if __name__ == '__main__':
    unittest.main()
