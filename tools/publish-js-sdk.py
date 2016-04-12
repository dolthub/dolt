#!/usr/bin/python

import os, subprocess, json
from distutils.version import LooseVersion

def main():
  os.chdir('js')
  deployed_version = LooseVersion(
    subprocess.check_output(['npm', 'info', '@attic/noms', 'version']).strip())
  with open('package.json') as pkg:
    data = json.load(pkg)
  new_version = LooseVersion(data['version'])
  print 'Old version: %s, New version: %s' % (deployed_version, new_version)
  if new_version > deployed_version:
    subprocess.check_call(['npm', 'publish'])

if __name__ == '__main__':
  main()
