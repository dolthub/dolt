#!/usr/bin/python

import os, subprocess

def main():
  os.chdir('js')
  print os.getcwd()
  current_deployed_version = subprocess.check_output(['npm', 'info', '@attic/noms', 'version'])
  current_deployed_major_version = current_deployed_version.split('.')[0]
  log = subprocess.check_output(['git', 'log', '--oneline', '--', '.'])
  new_major_version = len(log.splitlines())
  print 'Old version: %s, New version: %s', (current_deployed_version, new_major_version)
  if int(new_major_version) > int(current_deployed_major_version):
      subprocess.check_call(['npm', 'version', '%s.0.0' % new_major_version])
      # subprocess.check_call(['npm', 'publish'])
      subprocess.check_call(['npm', 'whomai'])
      print 'Not pushing this time. Maybe next time?'

if __name__ == '__main__':
  main()
