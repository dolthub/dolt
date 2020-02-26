## Compatibility Tests

These tests attempt to ensure forward and backward compatibility for Dolt versions.

The testing script `runner.sh` checks out and builds older Dolt release versions to ensure that they can read data from
newer repositories and vice-versa.
A test directory `/env_test` is created outside of source control to preserve the environment across
`git checkout` commands.

For each Dolt release version listed in `versions.txt`, `runner.sh` creates a legacy Dolt repository using the
`/test_files/setup_repo.sh` script in a directory named with the corresponding version.
An additional Dolt repository is created using Dolt built from the initial git branch.
BATS tests, located in `test_files/bats/`, are used to verify the forward and backward compatibility of all Dolt versions
and the repositories created with those versions.

### Updating

The BATS tests used to verify compatibility are inherently fragile.
Our primary integration tests in `/dolt/bats/` setup and tear down their environment for each test.
Because the tests rely on creating a repo with one version of Dolt and running BATS tests with a different version, 
we cannot isolate their environment without building Dolt twice per test or setting up a different Dolt repo per test.
The initial version of these tests does all write operations in the `setup_repo.sh` script, and limits state modifications
within the BATS test to `dolt checkout` branch changes. Take care when editing the BATS tests to follow this pattern.