#!/usr/bin/python

import os, subprocess
from distutils.version import LooseVersion

def main():
  os.chdir('js')
  deployed_version = LooseVersion(
    subprocess.check_output(['npm', 'info', '@attic/noms', 'version']).strip())
  changes = subprocess.check_output(
    ['git', 'rev-list', '--no-merges', '--count', 'HEAD', '--', './']).strip()
  new_version = LooseVersion('%s.0.0' % changes)
  print 'Old version: %s, New version: %s' % (deployed_version, new_version)
  if new_version > deployed_version:
    # subprocess.check_call(['npm', 'version', str(new_version)])
    # subprocess.check_call(['npm', 'publish'])
    print 'Not publishing until I know it works'

if __name__ == '__main__':
  main()
