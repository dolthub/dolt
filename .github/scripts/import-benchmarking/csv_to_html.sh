#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: csv_to_html.sh <csv_file> <html_file>"
fi

gw="$GITHUB_WORKSPACE"
in="$1"
out="$2"

echo "<table>" > "$out"
print_header=true
while read line; do
  if "$print_header"; then
    #echo "  <tr><th>$line" | sed -e 's/:[^,]*\(,\|$\)/<\/th><th>/g' >> "$out"
    echo "  <tr><th>${line//,/</th><th>}</th></tr>" >> "$out"
    print_header=false
    continue
  fi
  echo "  <tr><td>${line//,/</td><td>}</td></tr>" >> "$out"
done < "$in"
echo "</table>" >> "$out"

cat "$out"

echo "::set-output name=html::$(echo $out)"
