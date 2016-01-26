#!/usr/bin/python

import noms.staging as staging


if __name__ == '__main__':
	staging.Main('splore', staging.GlobCopier('out.js', 'index.html', 'styles.css'))
