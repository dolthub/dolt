#!/usr/bin/python

import argparse, os, os.path, subprocess, sys, shutil, urlparse


def main():
  parser = argparse.ArgumentParser(description='Dependency snapshotter')
  parser.add_argument('url')
  parser.add_argument('--path', help='path to store the dependency at, defaults to vendor/[url without protocol]')
  parser.add_argument('--version', default='HEAD', help='version of the dependency to snapshot, defaults to HEAD')

  args = parser.parse_args()

  url = urlparse.urlparse(args.url)
  if url.scheme == '':
    print 'Invalid url: no scheme'
    sys.exit(1)

  if not os.path.isdir('.git'):
    print '%s must be run from the root of a repository' % sys.argv[0]
    sys.exit(1)

  path = url.path
  if path.startswith('/'):
    path = path[1:]

  depdir = args.path
  if depdir == None:
    depdir = os.path.join('vendor', url.netloc, path)

  shutil.rmtree(depdir, True)
  parent = os.path.dirname(depdir)
  if not os.path.isdir(parent):
    os.makedirs(parent)
  os.chdir(parent)

  # Kinda sucks to clone entire repo to get a particular version, but:
  # http://stackoverflow.com/questions/3489173/how-to-clone-git-repository-with-specific-revision-changeset
  subprocess.check_call(['git', 'clone', args.url])

  os.chdir(os.path.basename(depdir))
  subprocess.check_call(['git', 'reset', '--hard', args.version])
  head = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()

  f = open('.version', 'w')
  f.write('%s\n%s\n' % (args.url, head))
  f.close()

  shutil.rmtree('.git')


if __name__ == '__main__':
  main()
