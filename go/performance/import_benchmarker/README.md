## Import Benchmarker

This library is used to benchmark `dolt table import` on csv/json files and `dolt sql` on .sql files. It uses 
the Go testing.B package to execute the relevant dolt commands.

### Test Files

This package uses several test files that are stored in a private S3 bucket (import-benchmarking-github-actions-results)
which represent different sort order, primary keys, etc.

The benchmarker supports custom configurations which runs different import jobs against a `dolt` database or a MySQL 
server. The parameters of each job and the overall config file are specified in `config.go`. 

Note that if you run the benchmarker without a filepath than the benchmarker will generate a sample file for you. It is 
best to stick with the default files used in the production benchmarking system to maintain a sense of consistency.\

### Notes

* You should name your table "test" in the MySQL schema file.