
This directory contains a set of corrupt databases that are used to test the behavior of fsck,
and perhaps other tools in the future. Please catalog the contents of each database here.
(.dolt/* -> test-dir)

## bad_commit 
  This database contains a commit (rlmgv0komq0oj7qu4osdo759vs4c5pvg) that has contents in the database
  that do not have contents which matches the address (Actual data in table file: gpphmuvegiedtjtbfku4ru8jalfdk21u).
  This represents updating the author of that commit maliciously.

## bad_journal_crc
  This database contains a journal file which has been altered to have an object (7i48kt4h41hcjniri7scv5m8a69cdn13)
  which has a bad CRC. The object in question doesn't trip any problems until it's fully loaded, which indicates
  that the CRC for the journal record is correct, but the data is not.
