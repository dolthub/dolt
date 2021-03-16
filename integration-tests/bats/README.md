# BATS - Bash Automated Testing System #

BATS is used to integration test `dolt`. Our BATS tests started as a humble suite of integration tests. Over two years
of development the suite has grown to over 1,000 tests. When we find a customer facing bug in the `dolt` command line or
SQL implementation, we cover it with a BATS test. These tests are run on every `dolt` PR on Mac, Windows, and Linux using
GitHub Actions. 

These tests are also useful documentation. If you are wondering how a certain command or feature works in practice,
using `grep` to find the appropriate BATS test can give you some simple examples of happy path and error case behavior.

The naming conventions for the test files have evolved over time. Generally, the files are named after the feature the
file intends to test. However, some of the early tests are named after the schema of the table they implement 
ie. `1pk5col-ints.bats`. These files were implemented to reuse setup and teardown logic. This scheme was quickly 
abandoned but the legacy remains.

If you find a bug in `dolt`, we would love a skipped bats test PR in addition to a GutHub issue.

# Running for yourself

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

## More Information

We published a [blog entry](https://www.dolthub.com/blog/2020-03-23-testing-dolt-bats/) on BATS with 
more information and some useful tips and tricks.
