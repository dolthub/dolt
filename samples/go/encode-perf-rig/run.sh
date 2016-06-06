#!/bin/sh

echo
echo small numbers save a lot of space as strings
./encode-perf-rig -encoding=string -from=0 -iterations=100000
./encode-perf-rig -encoding=binary -from=0 -iterations=100000
echo
echo once the numbers are larger than 1m, the space savings are gone
./encode-perf-rig -encoding=string -from=900000 -to=1000000
./encode-perf-rig -encoding=string -from=1000000 -to=1100000
./encode-perf-rig -encoding=binary -from=900000 -to=1000000
./encode-perf-rig -encoding=binary -from=1000000 -to=1100000
echo
echo binary is always faster
./encode-perf-rig -encoding=string -from=0 -to=1000000 -by=2
./encode-perf-rig -encoding=binary -from=0 -to=1000000 -by=2
echo
echo larger numbers are the same - binary still faster, but now strings are bigger than binary
./encode-perf-rig -encoding=string -from=1000000000000 -to=1000000001000 -by=2
./encode-perf-rig -encoding=binary -from=1000000000000 -to=1000000001000 -by=2
echo 
echo if we special case non-floating numbers we get some space savings
./encode-perf-rig -encoding=binary -from=0 -to=10000000 -by=1
./encode-perf-rig -encoding=binary-int -from=0 -to=10000000 -by=1
echo
echo then if we special case small integers we get even more space savings
./encode-perf-rig -encoding=binary-varint -from=0 -to=10000000 -by=1
