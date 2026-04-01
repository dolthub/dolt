## Compatibility Tests

These tests attempt to ensure forward and backward compatibility for Dolt versions.

For each Dolt release version listed in `test_files/backward_compatible_versions.txt`, a legacy
repository is created using the corresponding release and populated with data from
`test_files/setup_repo.sh`.  Then, using a Dolt client build at HEAD, a series of BATS tests are run
against each legacy repository.

To test forward compatibility, a repository is created and populated using Dolt built at HEAD.  For
each Dolt release version listed in `test_files/forward_compatible_versions.txt`, the same BATS
tests are run against the repo created with Dolt at HEAD.

Then there is a third set of tests that test bidirectional compatibility, whether older and current
clients can alternate doing writes on the same DB withut anything breaking. These tests do their own
initialization and don't rely on the `setup_repo.sh`. Forward compatibility is the limiting factor
for these tests (fewer versions are forward compatible at any given time), so that's the versions we
do bidirectional testing against.
