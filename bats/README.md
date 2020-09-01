# BATS - Bash Automated Testing System #

We are going to use bats to test the dolt command line. 

First you need to install bats. 
```
npm install -g bats
```
Then, go to the directory with the bats tests and run: 
```
bats . 
```
This will run all the tests. Specify a particular .bats file to run only those tests.

## Here Docs

BATS tests in Dolt make extensive use of [Here Docs](https://en.wikipedia.org/wiki/Here_document).
Common patterns include piping SQL scripts to `dolt sql`:  
```sql
    dolt sql <<SQL
CREATE TABLE my_table (pk int PRIMARY KEY);
SQL
```
And creating data files for import:
```sql
    cat <<DELIM > data.csv
pk,c1,c2
1,1,1
2,2,2
DELIM
    dolt table import -c -pk=pk my_table data.csv
```

## Skipped BATS

Various tests are skipped as TODOs and/or as documentation of known bugs. Eg: 
```sql
@test "..." {
    ...
    skip "this test is currently failing because..."
}
```
Skipped BATS can still be partially useful for testing as they execute normally up to `skip` statement.