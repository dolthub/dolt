#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

import argparse
import glob
import hashlib
import os
import os.path
import re
import shutil

def Main(projectName, stagingFunction):
    """Main should be called by all staging scripts when executed.

    Main takes a project name and a callable. It creates a staging directory for
    your project and then runs the callable, passing it the path to the
    newly-created staging directory.

    For the common case of simply copying a set of files into the staging
    directory, use GlobCopier:

    #!/usr/bin/python

    import noms.staging as staging

    if __name__ == '__main__':
        staging.Main('nerdosphere', staging.GlobCopier('index.html', 'styles.css', '*.js'))
    """
    parser = argparse.ArgumentParser(description='Stage build products from this directory.')
    parser.add_argument('staging_dir',
                        metavar='path/to/staging/directory',
                        type=_dir_path,
                        help='top-level dir into which project build products are staged')
    args = parser.parse_args()
    project_staging_dir = os.path.join(args.staging_dir, projectName)

    normalized = os.path.realpath(project_staging_dir)
    if not _is_sub_dir(project_staging_dir, args.staging_dir):
        raise Exception(project_staging_dir + ' must be a subdir of ' + args.staging_dir)

    if not os.path.exists(normalized):
        os.makedirs(normalized)
    stagingFunction(normalized)


def run_globs(staging_dir, globs, exclude):
    for pattern in globs:
        for f in glob.glob(pattern):
            if os.path.isdir(f):
                continue
            from_dir, name = os.path.split(f)
            if name in exclude:
                continue
            to_dir = os.path.join(staging_dir, from_dir)
            if not os.path.exists(to_dir):
                os.makedirs(to_dir)
            yield (f, to_dir)


def rename_with_hash(f, to_dir, rename_dict):
    with open(f) as fh:
        sha = hashlib.sha256()
        sha.update(fh.read())
        digest = sha.hexdigest()
    basename = os.path.basename(f)
    name, ext = os.path.splitext(basename)
    new_name = '%s.%s%s' % (name, digest[:20], ext)
    rename_dict[basename] = new_name
    shutil.move(os.path.join(to_dir, basename), os.path.join(to_dir, new_name))


def GlobCopier(*globs, **kwargs):
    '''
    Returns a function that copies files defined by globs into a staging dir.

    Arguments:
    - Zero or more globs used to determine which files to copy.

    Keyword arguments:
    - rename (bool) - If True then the files gets renamed to name.%%hash.ext
    - index_file (str) - If present then this file is copied to the staging dir
      and its content is updated where the paths to the files are updated to the
      renamed file paths.
    '''
    exclude = ('webpack.config.js',)
    rename = 'rename' in kwargs and kwargs['rename']
    def stage(staging_dir):
        if rename:
            rename_dict = dict()
        for f, to_dir in run_globs(staging_dir, globs, exclude):
            shutil.copy2(f, to_dir)
            if rename:
                rename_with_hash(f, to_dir, rename_dict)

        # Update index_file and write it to to_dir.
        if 'index_file' not in kwargs:
            return
        index_file = kwargs['index_file']
        from_dir, name = os.path.split(index_file)
        to_dir = os.path.join(staging_dir, from_dir)
        with open(index_file, 'r') as f:
            data = f.read()
        if rename:
            for old_name, new_name in rename_dict.iteritems():
                r = re.compile(r'\b%s\b' % re.escape(old_name))
                data = r.sub(new_name, data)
        with open(os.path.join(to_dir, name), 'w') as f:
            f.write(data)

    return stage


def _dir_path(arg):
    normalized = os.path.realpath(os.path.abspath(arg))
    if os.path.exists(normalized) and not os.path.isdir(normalized):
        raise ValueError(arg + ' is not a path to a directory.')
    return normalized


def _is_sub_dir(subdir, directory):
    # Need the path-sep at the end to ensure that commonprefix returns the correct result below.
    directory = os.path.join(os.path.realpath(directory), '')
    subdir = os.path.realpath(subdir)

    # return true, if the common prefix of both is equal to directory e.g.  /a/b/c/d.rst and
    # directory is /a/b, the common prefix is /a/b
    return os.path.commonprefix([subdir, directory]) == directory
