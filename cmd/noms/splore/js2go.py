#!/usr/bin/env python

import sys

def main():
    if len(sys.argv) != 4:
        print('usage: %s <source> <package> <varname>' % (sys.argv[0]))
        return

    jsf, pkg, varname = sys.argv[1:]

    sys.stdout.write('package %s\n\n' % (pkg,))
    sys.stdout.write('const %s = "' % (varname,))
    with open(jsf, 'r') as js:
        sys.stdout.write(
                js.read()
                .replace('\\', '\\\\')
                .replace('"', '\\"')
                .replace('\n', '" +\n"'))
    sys.stdout.write('"')

if __name__ == '__main__':
    main()
