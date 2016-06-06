#!/bin/sh

RUN_EXEC="/usr/local/bin/node dist/main.js"
echo
echo small numbers save a lot of space as strings
$RUN_EXEC --encoding=string --from=0 --to=100000
$RUN_EXEC --encoding=binary --from=0 --to=100000
echo
echo once the numbers are larger than 1 trillion, the space savings are gone
$RUN_EXEC --encoding=string --from=999999999000 --to=1000000000000
$RUN_EXEC --encoding=string --from=1000000000000 --to=1000000001000
$RUN_EXEC --encoding=binary --from=999999999000 --to=1000000000000
$RUN_EXEC --encoding=binary --from=1000000000000 --to=1000000001000
echo
echo binary is almost always faster in Go but JS appears differently
$RUN_EXEC --encoding=string --from=0 --to=1000000 --by=2
$RUN_EXEC --encoding=binary --from=0 --to=1000000 --by=2
echo
echo larger numbers - strings are now bigger than binary, but binary is slowing down
$RUN_EXEC --encoding=string --from=100000000000000 --to=100000000001000 --by=2
$RUN_EXEC --encoding=binary --from=100000000000000 --to=100000000001000 --by=2
echo 
echo if we special case non-floating numbers we get some space savings
$RUN_EXEC --encoding=binary --from=0 --to=10000000 --by=1
$RUN_EXEC --encoding=binary-int --from=0 --to=10000000 --by=1
$RUN_EXEC --encoding=string --from=0 --to=10000000 --by=1
echo
echo then if we special case small integers we get even more space savings
$RUN_EXEC --encoding=binary-varint --from=0 --to=10000000 --by=1
