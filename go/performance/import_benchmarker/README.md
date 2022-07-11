## Import Benchmarker

This library is used to benchmark `dolt table import` with csv/json and `dolt sql` with SQL data. It uses 
the Go testing.B package to run the import command. 

### Test Files

This package uses several test files that are stored in a private S3 bucket (import-benchmarking-github-actions-results)
which represent different sort order, primary keys, etc.

You can use the sample-config in the cmd package to benchmark against a sample set of files. If a filepath
is not specified, Dolt will generate a random test file for you.