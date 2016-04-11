#!/usr/bin/python

import os, subprocess
from distutils.version import LooseVersion

def main():
  os.chdir('js')
  deployed_version = LooseVersion(
    subprocess.check_output(['npm', 'info', '@attic/noms', 'version']).strip())
  new_version = LooseVersion(
    subprocess.check_output(['npm', 'info', '.', 'version']).strip())
  print 'Old version: %s, New version: %s' % (deployed_version, new_version)
  if new_version > deployed_version:
    #   subprocess.check_call(['npm', 'version', '%s' % str(new_version)])
      # subprocess.check_call(['npm', 'publish'])
      subprocess.check_call(['npm', 'whomai'])
      print 'Not pushing this time. Maybe next time?'

if __name__ == '__main__':
  main()
