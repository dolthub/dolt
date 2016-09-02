#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import os.path, shutil

def Peers(me, dstDir):
    """Peers copies the peers of me into dstDir.

    Peers looks for files, directories and symlinks next to me
    and copies them (with the same basenames) to dstDir, which is
    presumed to exist.
    """
    myDir = os.path.dirname(os.path.abspath(me))
    names = os.listdir(myDir)
    for basename in names:
        src = os.path.join(myDir, basename)
        dst = os.path.join(dstDir, basename)
        if os.path.samefile(me, src):
            continue

        if os.path.islink(src):
            linkto = os.readlink(src)
            os.symlink(linkto, dst)
        elif os.path.isfile(src):
            shutil.copy2(src, dst)
        elif os.path.isdir(src):
            shutil.copytree(src, dst)
        else:
            raise Exception("Unknown file type at " + src)
