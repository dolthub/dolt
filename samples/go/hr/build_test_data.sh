#!/bin/sh

if [ -d test-data ]; then
	mv test-data test-data.bak
fi

./hr --ds test-data::hr add-person 7 "Aaron Boodman" "Chief Evangelism Officer"
./hr --ds test-data::hr add-person 13 "Samuel Boodman" "VP, Culture"
