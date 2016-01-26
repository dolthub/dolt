# Noms build script helpers

These are helper functions for writing your Noms app build and staging scripts.

## Writing your scripts

### Build scripts
Your build script must be named *build.py*. It will be discovered by the system and executed in the directory in which it's found. It must require no arguments, though environment variables will propagate in.

### Staging scripts
After your build script gets run, we'll run your staging script -- the purpose of which is to take your build products and put them in a directory that's ready to be packaged and deployed somewhere. This script must be called *stage.py* and take as its sole argument the path to a directory where all project code is being staged.

### Libraries
We have provided a library to make writing your staging scripts easier. Example usage:
```python
	#!/usr/bin/python

	import noms.staging as staging

	if __name__ == '__main__':
		staging.Main('nerdosphere', staging.GlobCopier('index.html', 'styles.css', '*.js'))
```
Importing and using `noms.staging` handles determining where you should stage your code and creating the necessary directories for you. You just pass it the name of your project and a function that knows how to stage your build artifacts, given a path under which to put everything.


## Develop

* To run unittests: `python -m unittest discover -p "*_test.py" -s $GOPATH/src/github.com/attic-labs/noms/tools`
