## Compatibility Tests

These tests attempt to ensure forward and backward compatibility for Dolt versions.

For each Dolt release version listed in `test_files/backward_compatible_versions.txt`, a legacy repository is created 
using the corresponding release and populated with data from `test_files/setup_repo.sh`.
Then, using a Dolt client build at HEAD, a series of BATS tests are run against each legacy repository. 

To test forward compatibility, a repository is created and populated using Dolt built at HEAD.
For each Dolt release version listed in `test_files/forward_compatible_versions.txt`, the same BATS tests are run 
against the repo created with Dolt at HEAD.
