#!/usr/bin/python

# Copyright 2016 Attic Labs, Inc. All rights reserved.
# Licensed under the Apache License, version 2.0:
# http://www.apache.org/licenses/LICENSE-2.0

# This script ensures our license header is present on all the files git knows
# about within the current directory.
#
# It is safe to re-run this file on already-processed files.

import os
import re
import subprocess
import shutil
import tempfile

licenseRows = [
    'Copyright 2016 Attic Labs, Inc. All rights reserved.',
    'Licensed under the Apache License, version 2.0:',
    'http://www.apache.org/licenses/LICENSE-2.0',
]

comment_markers = {
    'go': ('', '// ', ''),
    'js': ('', '// ', ''),
    'py': ('', '# ', ''),
    'html': ('<!--', '  ', '-->'),
    'css': ('/**', ' * ', ' */'),
}


def main():
    files = subprocess.check_output(['git', 'ls-files']).split('\n')
    for n in files:
        if n != '' and not n.startswith('vendor/') and (
                not n.endswith('.min.js')):
            _, ext = os.path.splitext(n)
            if ext == '':
                continue
            ext = ext[1:]
            pattern = buildLicensePattern(ext)
            if pattern != None:
                with open(n, 'r+') as f:
                    processFile(f, ext, pattern)


def processFile(f, ext, pattern):
    '''Updates the license block in file |f|.'''
    content = f.read()
    f.seek(0)
    f.truncate()
    replacement = re.sub(pattern, getLicense(ext), content)
    f.write(replacement)


def buildLicensePattern(ext):
    '''
    Builds a regex pattern that matches license blocks in files with extension
    |ext|.
    '''
    markers = comment_markers.get(ext)
    if markers is None:
        return None

    (first, mark, last) = [re.escape(m) for m in markers]

    prefix = ''
    # The first line must include the copyright string to avoid picking up
    # random other comment blocks at head of file.
    head = mark + r'Copyright \d+ (The Noms Authors|Attic Labs).*\n'
    body = '(' + mark + r'.*\n)*'
    suffix = ''

    if first != '':
        prefix = first + r'\n'
    if last != '':
        suffix = last + r'\n'

    # We want to make sure shebang files stay at head of file.
    shebang = r'(?P<shebang>\#\!.+\n+|)'

    # Allow flow annotations
    flow = r'(?P<flow>// @flow\n+|)'

    # Doctype header
    doctype = r'(?P<doctype><\!doctype html>\n+|)'

    return ('^' + shebang + doctype +
        '(' + prefix + head + body + suffix + r'\n)?' + flow)


def getLicense(ext):
    '''Gets the license block for files with extension |ext|.'''
    (first, mark, last) = comment_markers[ext]
    result = '\n'.join([mark + line for line in licenseRows])
    if first != '':
        result = first + '\n' + result
    if last != '':
        result = result + '\n' + last
    return r'\g<shebang>' + r'\g<doctype>' + result + '\n\n\g<flow>'


if __name__ == '__main__':
    main()
