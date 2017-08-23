#!/bin/sh
#
# Installation script for ipfs. It tries to move $bin in one of the
# directories stored in $binpaths.

bin=ipfs
binpaths="/usr/local/bin /usr/bin"

# This variable contains a nonzero length string in case the script fails
# because of missing write permissions.
is_write_perm_missing=""

for binpath in $binpaths; do
  if mv "$bin" "$binpath/$bin" 2> /dev/null; then
    echo "Moved $bin to $binpath"
    exit 0
  else
    if [ -d "$binpath" -a ! -w "$binpath" ]; then
      is_write_perm_missing=1
    fi
  fi
done

echo "We cannot install $bin in one of the directories $binpaths"

if [ -n "$is_write_perm_missing" ]; then
  echo "It seems that we do not have the necessary write permissions."
  echo "Perhaps try running this script as a privileged user:"
  echo
  echo "    sudo $0"
  echo
fi

exit 1
