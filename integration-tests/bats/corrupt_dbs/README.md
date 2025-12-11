
This directory contains a set of corrupt databases that are used to test the behavior of fsck,
and perhaps other tools in the future. Please catalog the contents of each database here.
(.dolt/* -> test-dir)

## bad_commit 
  This database contains a commit (rlmgv0komq0oj7qu4osdo759vs4c5pvg) that has contents in the database
  that do not have contents which matches the address (Actual data in table file: gpphmuvegiedtjtbfku4ru8jalfdk21u).
  This represents updating the author of that commit maliciously.

## bad_journal_crc
  This database contains a journal file which has been altered to have an object (7i48kt4h41hcjniri7scv5m8a69cdn13)
  which has a bad CRC. The object in question doesn't trip any problems until it's fully loaded, which indicates that the CRC for the journal record is correct, but the data is not.

## journal_data.bin
  This is a binary file which constitutes a valid journal root hash record followed by a
  chunk record (100 bytes of random data). This file can be appended to assist in testing for
  dataloss. We parse journal files until we can't parse them any longer, so you need to append
  null bytes or random data to the end of a journal file to get into that state. That does
  not constitute dataloss though - which is why you append this file to the journal because
  these bytes are parsable. 

## missing_closure_object
  This is a manually mangled database. It contains three empty commits. The second commit's closure
  object has been removed using the:
  `dolt admin journal-inspect --filter d92u2dpnhocp5pv4pn7vgm9fs30vdv94 .dolt/noms/vvvvvvv*`

  Original journal preserved in file: vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv_saved_2025_12_10_173658
