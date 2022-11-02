 # Type Change Tests

### What are these tests?

All of the tests contained in this directory that begin with `modify_` are a part of the collection of tests observing MySQL 8.0's behavior when changing a type from one to another. The goal of the tests are to ensure that we, as close as we can, copy MySQL's behavior when changing between the different types. This is necessary as the behavior of each modification is not fully documented, therefore the only way to know how MySQL operates is to painstakingly try every combination. It is not feasible to try the entire set of type changes, and so we aim to try as wide a range of conversions as possible without extending full-repository testing times to an unreasonable amount.

The `common_test` file is the heart of these tests, containing the running logic that each file references. The testing files are just a large selection of tests that pass their contents as parameters to the `RunModifyTypeTests` function. In addition, the common file hosts any helper functions that the test files may need. This is due to the generated nature of the test files, although it is not required that all functions live in the common file. It is probable that a helper in one file will be useful in another, thus it's a convenient location for all.

Additionally, every test assumes that the table has the following signature: `CREATE TABLE test(pk BIGINT PRIMARY KEY, v1 X);`, with `v1` having the type defined by the test.

### How were these files generated?

Each file follows a basic template: copyright header, package name, imports, test function name, test array, and call to `RunModifyTypeTests`. All but the function name and test array were static strings prepended and appended accordingly. The function name was added by hand for each file.

The test array is an array of the predefinied struct `ModifyTypeTest`.
* `FromType`: The type that we start with and are converting from. This is the same for every test in a file.
* `ToType`: The type that we are converting to.
* `InsertValues`: These are the values that are being inserted into the column before the conversion. These are necessary as all conversions are valid when there are no rows. For the statement `INSERT INTO test VALUES %s;`, this string is copied to `%s`. All values should normally insert for the given type, as the insert is expected to succeed.
* `SelectRes`: This is an array of values that we expect from running `SELECT v1 FROM test ORDER BY pk;`. These are all expected to be the widest type for a particular _base_ type. For example, if you're expecting an unsigned integer with the value 1, then your value would be `uint64(1)`, even if the column actually returns `uint8(1)`. This simplifies comparisons, as we enforce that each type returns the correct type elsewhere.
* `ExpectedErr`:  If the conversion should fail, then this will be true (skipping the aforementioned `SELECT` statement). Otherwise, it should be false.

To generate each test file, the process was generally as follows.
1) Select the `FromType` for the file you are generating.
2) Determine the set of values to be tested. For signed integer types, this was negative values up to the lowest value represented by that type, going to positive values up to the highest value represented for that type. Then adding values in the middle, including 0, and other "bit maximums" if they fit (such as `2^7-1`, `2^8-1`, `2^16-1`, `-2^7`, etc.).
3) Determine the combinations of values from the set to be tested. For signed integers, this was essentially starting at zero and fanning out in both directions. As an example, given the numbers `-2|-1|0|1|2`, the tested sets were `0`, `0|1`, `-1|0|1`, `0|1|2`, and `-2|-1|0|1|2`. This strategy does mean that an invalid value of a smaller number could falsely paint a type as being completely incompatible. For example, if `0` did not convert between the integer type and another type, then all conversions would fail since they all contain `0`. In practice though, the smallest values seem to be the most convertible.

From here, a list of all the types to test was referenced (see the bottom of the README), and a MySQL client created a table, inserted the one of the value sets, and converted the target column (`v1`) to one of the referenced types. If the change failed, then we record that there was an error (details on the error aren't necessary). If it succeeded, we ran the same `SELECT` statement mentioned earlier in the README, and recorded the output. This then was formatted into a valid `ModifyTypeTest` struct, and appended to a string buffer, which was written to a file once all of the permutations were tested.

Once the test file was moved to Dolt, all of the tests were ran to see if the placeholder logic was sufficient for that conversion, and changed if it wasn't. In some cases, Dolt and MySQL slightly differ (such as with `FLOAT` rounding, giving `1.124005` vs `1.124005042`), so the tests were manually adjusted to reflect Dolt's comparable output rather than MySQL's exact output.

### Where is the generation program?

No program is provided, as the source was constantly changed to generate each test file. This is because the bulk of the code dealt with generating the permutations of the values, along with determining which values to use (which was an arbitrary decision). Although a more formal program could have been written with more reusable code, it was determined to not be worth the effort at the time. Using the information given above, it is relatively trivial to construct your own program to generate test files. And even that is just a time saver—you can just as easily write tests by hand. The test format was chosen to be simple to read and simple to write. Generating tests through a program is **NOT REQUIRED**.

### Type Reference

Here are all of the types that are tested from/to. Similar types are grouped together:

TINYINT<br>
SMALLINT<br>
MEDIUMINT<br>
INT<br>
BIGINT<br>
<br>
TINYINT UNSIGNED<br>
SMALLINT UNSIGNED<br>
MEDIUMINT UNSIGNED<br>
INT UNSIGNED<br>
BIGINT UNSIGNED<br>
<br>
FLOAT<br>
DOUBLE<br>
<br>
DECIMAL(1,0)<br>
DECIMAL(15,0)<br>
DECIMAL(30,0)<br>
DECIMAL(65,0)<br>
DECIMAL(1,1)<br>
DECIMAL(15,1)<br>
DECIMAL(30,1)<br>
DECIMAL(65,1)<br>
DECIMAL(15,15)<br>
DECIMAL(30,15)<br>
DECIMAL(65,15)<br>
DECIMAL(30,30)<br>
DECIMAL(65,30)<br>
<br>
BIT(1)<br>
BIT(8)<br>
BIT(16)<br>
BIT(24)<br>
BIT(32)<br>
BIT(48)<br>
BIT(64)<br>
<br>
TINYBLOB<br>
BLOB<br>
MEDIUMBLOB<br>
LONGBLOB<br>
<br>
TINYTEXT<br>
TEXT<br>
MEDIUMTEXT<br>
LONGTEXT<br>
<br>
CHAR(1)<br>
CHAR(10)<br>
CHAR(100)<br>
CHAR(255)<br>
<br>
BINARY(1)<br>
BINARY(10)<br>
BINARY(100)<br>
BINARY(255)<br>
<br>
VARCHAR(1)<br>
VARCHAR(10)<br>
VARCHAR(100)<br>
VARCHAR(255)<br>
VARCHAR(1023)<br>
VARCHAR(4095)<br>
VARCHAR(16383)<br>
<br>
VARBINARY(1)<br>
VARBINARY(10)<br>
VARBINARY(100)<br>
VARBINARY(255)<br>
VARBINARY(1023)<br>
VARBINARY(4095)<br>
VARBINARY(16383)<br>
<br>
YEAR<br>
<br>
DATE<br>
<br>
TIME<br>
<br>
TIMESTAMP<br>
DATETIME<br>
<br>
ENUM('A')<br>
ENUM('B')<br>
ENUM('C')<br>
ENUM('A','B')<br>
ENUM('A','C')<br>
ENUM('B','C')<br>
ENUM('A','B','C')<br>
ENUM('C','A','B')<br>
<br>
SET('A')<br>
SET('B')<br>
SET('C')<br>
SET('A','B')<br>
SET('A','C')<br>
SET('B','C')<br>
SET('A','B','C')<br>
SET('C','A','B')<br>
