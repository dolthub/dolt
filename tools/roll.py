#!/usr/bin/python

import os, os.path, subprocess, sys, shutil


def main():
  if len(sys.argv) != 2:
    print 'Usage: %s github.com/foo/bar' % sys.argv[0]
    sys.exit(1)

  dep = sys.argv[1]
  if dep.startswith('http'):
    print 'Argument should not start with "http" or "https" (https is implied)'
    sys.exit(1)

  if not os.path.isdir('.git'):
    print '%s must be run from the root of a repository' % sys.argv[0]
    sys.exit(1)

  depdir = os.path.join('vendor', dep)
  shutil.rmtree(depdir, True)
  parent = os.path.dirname(depdir)
  if not os.path.isdir(parent):
    os.makedirs(parent)
  os.chdir(parent)

  subprocess.check_call(['git', 'clone', '--depth=1', 'https://%s' % dep])
  head = subprocess.check_output(['git', 'rev-parse', 'HEAD'])

  os.chdir(os.path.basename(depdir))
  f = open('.version', 'w')
  f.write(head)
  f.close()

  shutil.rmtree('.git')


if __name__ == '__main__':
  main()
