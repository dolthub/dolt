#!/usr/bin/env bash

out=${1-tlds_iana.go}

list=$(mktemp)

wget https://data.iana.org/TLD/tlds-alpha-by-domain.txt -O $list

header=$(head -1 $list)

cat > $out <<EOF
package isdomain

// $header
var TLDs = map[string]bool{
EOF

grep -v '^#' $list | while read tld; do
    echo -e "\t\"$tld\": true,"
done >> $out

echo '}' >> $out

gofmt -w $out


