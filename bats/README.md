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

# Test coverage needed for: #
* large tables 
* dolt login
* schema diffs/merge/conflicts
