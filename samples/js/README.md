# JS Samples

This directory has a `package.json` and a `node_modules directory`. This is so that we do not have
to do the slow `npm install` in every sample. Instead each sample have no dependencies and the
dependencies are added to the shared parent directory. If we instead had complete standalone samples
in every subdirectory it would would mean that we have to have n copies of all the node modules we
depend on. This takes a long time to install and it takes up a lot of disk space.

## Running a sample

First run `npm install` in this directory, then in the subdirectory.

```
cd js
npm install
cd ..
cd samples/js
npm install
cd fs
npm install
```

Then to run the sample:

```
node .
```

You only need to do the `npm install` calls once. After that you can use `npm run build` or
`npm run start` for dev mode.
