## Compatibility Tests

These tests attempt to ensure forward and backward compatibility for Dolt versions.

For each Dolt release version listed in `versions.txt`, `runner.sh` creates a legacy Dolt repository using the
`/test_files/setup_repo.sh` script in a directory named with the corresponding version.
An additional Dolt repository is created using Dolt built from the initial git branch.
BATS tests, located in `test_files/bats/`, are used to verify the forward and backward compatibility of all Dolt versions
and the repositories created with those versions.
