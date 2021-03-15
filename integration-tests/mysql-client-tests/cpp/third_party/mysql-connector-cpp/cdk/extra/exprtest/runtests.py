#!/usr/bin/python
# Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; version 2 of the
# License.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
# 02110-1301  USA

# Test runner for general driver expression parser

# The test executable must take 2 cmdline params as input:
# <type> <expr>
# where <type> is col, tab or all
#
# It must print to stdout either:
# OK
# Protobuf text encoded message
#
# Or
# ERROR
# Error message
#
# In both cases, it must exit with status 0. Any other exit status will be
# interpreted as non-parser related error and abort the test suite.
#
# No other messages should be printed, unless you will be exiting with a
# non-0 status
#

import sys
import os
import subprocess
import difflib

def run_test(exe, otag, tag, text, out):
    p = subprocess.Popen([exe, tag, text], stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output, _ = p.communicate()
    if out:
      out.write("## "+otag+":"+text+"\n")
      out.write(output+"\n")
    return output

def run_test_file(exe, tfile, outfile):
    count = 0
    for i, line in enumerate(open(tfile)):
        line = line.strip()
        if line.startswith("#") or not line:
            continue
        tag, sep, text = line.partition(":")
        if not sep:
            print "%s:%i:Unrecognized line: %s" % (tfile, i, line)
            sys.exit(1)
        if tag not in ["all", "col", "tab"]:
            print "%s:%i:Unknown test tag: %s" % (tfile, i, tag)
            sys.exit(1)

        if tag == "col" or tag == "all":
            result = run_test(exe, tag, "col", text, outfile)
        if tag == "tab" or tag == "all":
            if tag == "all":
                result2 = run_test(exe, tag, "tab", text, None)
                if result != result2:
                    outfile.write("ERROR: Output from tab mode does not match output from col\n")
                    outfile.write(result2+"\n")
            else:
                run_test(exe, tag, "tab", text, outfile)
        count += 1
    return count



if len(sys.argv) < 2:
    print "Syntax: %s <testbinary>" % (sys.argv[0])
    sys.exit(1)

exe = sys.argv[1]

import shutil
shutil.rmtree("testout", ignore_errors=True)
os.mkdir("testout")

print "Running expression suite with %s" % exe

count = 0
fails = 0
passes = 0
for f in os.listdir("t"):
    n = os.path.splitext(f)[0]

    category, _, casename = n.partition("_")
    if category not in ["expr"]:
        print "Filename for %s missing category, which can be one of: expr" % f
        sys.exit(1)

    print n, "...",
    sys.stdout.flush()

    inpath = os.path.join("t", f)
    outpath = os.path.join("testout", n+".result")
    respath = os.path.join("r", n+".result")

    outf = open(outpath, "w+b")
    count += run_test_file(exe, inpath, outf)
    outf.close()

    if not os.path.exists(respath):
        print "Test results file", respath, " is missing. Copy ", outpath, " to it after verifying"
        sys.exit(1)

    diff = difflib.unified_diff(open(respath).readlines(), open(outpath).readlines(), fromfile=respath, tofile=outpath)
    difftext = "".join([d for d in diff])
    if not difftext:
        passes += 1
        print "OK"
    else:
        fails += 1
        print "FAIL"
        print "One or more test cases in %s failed with the following differences:" % inpath
        print difftext

print "%i test cases executed from %i test suites" % (count, passes+fails)
print "%i failed, %i passed" % (fails, passes)
if fails > 0:
    sys.exit(1)

