#!/bin/sh
#
# Copyright (c) 2014 Christian Couder
# MIT Licensed; see the LICENSE file in this repository.
#

test_description="Test installation and some basic commands"

. lib/test-lib.sh

test_expect_success "current dir is writable" '
  echo "It works!" >test.txt
'

test_expect_success "ipfs version succeeds" '
  ipfs version >version.txt
'

test_expect_success "ipfs --version success" '
  ipfs --version
'

test_expect_success "ipfs version output looks good" '
  egrep "^ipfs version [0-9]+\.[0-9]+\.[0-9]" version.txt >/dev/null ||
  test_fsh cat version.txt
'

test_expect_success "ipfs versions matches ipfs --version" '
  ipfs version > version.txt &&
  ipfs --version > version2.txt &&
  diff version2.txt version.txt ||
  test_fsh ipfs --version

'

test_expect_success "ipfs version --all has all required fields" '
  ipfs version --all > version_all.txt &&
  grep "go-ipfs version" version_all.txt &&
  grep "Repo version" version_all.txt &&
  grep "System version" version_all.txt &&
  grep "Golang version" version_all.txt
'

test_expect_success "ipfs help succeeds" '
  ipfs help >help.txt
'

test_expect_success "ipfs help output looks good" '
  egrep -i "^Usage" help.txt >/dev/null &&
  egrep "ipfs <command>" help.txt >/dev/null ||
  test_fsh cat help.txt
'

test_expect_success "'ipfs commands' succeeds" '
  ipfs commands >commands.txt
'

test_expect_success "'ipfs commands' output looks good" '
  grep "ipfs add" commands.txt &&
  grep "ipfs daemon" commands.txt &&
  grep "ipfs update" commands.txt
'

test_expect_success "All commands accept --help" '
  echo 0 > fail
  while read -r cmd
  do
    $cmd --help </dev/null >/dev/null ||
      { echo $cmd doesnt accept --help; echo 1 > fail; }
  done <commands.txt

  if [ $(cat fail) = 1 ]; then
    return 1
  fi
'

test_expect_failure "All ipfs root commands are mentioned in base helptext" '
  echo 0 > fail
  cut -d" " -f 2 commands.txt | grep -v ipfs | sort -u | \
  while read cmd
  do
    grep "  $cmd" help.txt > /dev/null ||
      { echo missing $cmd from helptext; echo 1 > fail; }
  done

  if [ $(cat fail) = 1 ]; then
    return 1
  fi
'

test_expect_failure "All ipfs commands docs are 80 columns or less" '
  echo 0 > fail
  while read cmd
  do
    LENGTH="$($cmd --help | awk "{ print length }" | sort -nr | head -1)"
    [ $LENGTH -gt 80 ] &&
      { echo "$cmd" help text is longer than 79 chars "($LENGTH)"; echo 1 > fail; }
  done <commands.txt

  if [ $(cat fail) = 1 ]; then
    return 1
  fi
'

test_expect_success "'ipfs commands --flags' succeeds" '
  ipfs commands --flags >commands.txt
'

test_expect_success "'ipfs commands --flags' output looks good" '
  grep "ipfs pin add --recursive / ipfs pin add -r" commands.txt &&
  grep "ipfs id --format / ipfs id -f" commands.txt &&
  grep "ipfs repo gc --quiet / ipfs repo gc -q" commands.txt
'



test_done
