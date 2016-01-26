#!/usr/bin/python

import noms.staging as staging


if __name__ == '__main__':
	staging.Main('nerdosphere', staging.GlobCopier('*.js', '*.html', '*.css', '*.png'))
