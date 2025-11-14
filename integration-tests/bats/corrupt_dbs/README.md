
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

## bad_journal_crc_2
  This database contains a journal which has a record which has an invalide CRC

## bad_journal_invalid_record_len
  This database has a journal file which is healthy in everyway, but has an extra four bytes at
  the end which would typically be the length of the next record. The value exceeds the maximum
  number of bytes allowed in a record (5242881 - 5Mb +1). This should result in a log message
  at data load time, which we test for. This is an automatic recovery scenario so that database
  should work just fine after the message is logged.

## bad_journal_read_off_EOF
  This database contains a journal which a record length which reads off the end of the file.

## bad_journal_with_non_null_pad
  This database contains a fully packed journal file with the bytes "0x00,0x00,0x2A" added to the
  end. This is specifically to test for when the journal code peeks to read the journal record
  length and it returns EOF that there is no non-null data we skipped.

## happy_journal_with_null_pad
  This database is not corrupt. It has a journal file with 3 null bytes appended to the end. It is
  used for a positive test case to ensure that we are reading the journal record length at the
  end of the file and swallowing the EOF error appropriately. The negative test case is in the
  bad_journal_with_non_null_pad
  
