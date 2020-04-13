#!/bin/bash

set -eo pipefail

function download_release() {
  ver=$1
  dirname=binaries/"$ver"
  mkdir "$dirname"
  basename=dolt-"$PLATFORM_TUPLE"
  filename="$basename".tar.gz
  filepath=binaries/"$ver"/"$filename"
  url="https://github.com/liquidata-inc/dolt/releases/download/$ver/$filename"
  curl -L -o "$filepath" "$url"
  cd "$dirname" && tar zxf "$filename"
  echo "$dirname"/"$basename"/bin
}

get_platform_tuple() {
  OS=$(uname)
  ARCH=$(uname -m)
  if [ "$OS" != Linux -a "$OS" != Darwin ]; then
    echo "tests only support linux or macOS." 1>&2
    exit 1
  fi
  if [ "$ARCH" != x86_64 -a "$ARCH" != i386 -a "$ARCH" != i686 ]; then
    echo "tests only support x86_64 or x86." 1>&2
    exit 1
  fi
  if [ "$OS" == Linux ]; then
    PLATFORM_TUPLE=linux
  else
    PLATFORM_TUPLE=darwin
  fi
  if [ "$ARCH" == x86_64 ]; then
    PLATFORM_TUPLE="$PLATFORM_TUPLE"-amd64
  else
    PLATFORM_TUPLE="$PLATFORM_TUPLE"-386
  fi
  echo "$PLATFORM_TUPLE"
}

PLATFORM_TUPLE=`get_platform_tuple`

function export_tables() {
  dv=`dolt version`
  echo "exporting tables with dolt version $dv"
  for table in \
    case_details \
    cases \
    characteristics_age \
    characteristics_case_severity \
    characteristics_comorbid_condition \
    characteristics_occupation \
    characteristics_onset_date_range \
    characteristics_province \
    characteristics_sex \
    characteristics_wuhan_exposed \
    dolt_query_catalog \
    dolt_schemas \
    places
  do
    dolt table export "$table" "$table$1.csv"
    dolt sql -r csv -q "select * from $table" > "$table$1.sql.csv"
  done
}

function diff_tables() {
  for table in \
    case_details \
    cases \
    characteristics_age \
    characteristics_case_severity \
    characteristics_comorbid_condition \
    characteristics_occupation \
    characteristics_onset_date_range \
    characteristics_province \
    characteristics_sex \
    characteristics_wuhan_exposed \
    dolt_query_catalog \
    dolt_schemas \
    places
  do
    diff "$table-pre.csv" "$table-post.csv"
    diff "$table-pre.sql.csv" "$table-post.sql.csv"
  done
}

function cleanup() {
  popd
  rm -rf binaries
  rm -rf "corona-virus"
}
mkdir binaries
trap cleanup "EXIT"

bin=`download_release "v0.15.0"`
local_bin="`pwd`"/"$bin"

PATH="$local_bin":"$PATH" dolt clone Liquidata/corona-virus
pushd "corona-virus"
PATH="$local_bin":"$PATH" export_tables "-pre"
dolt migrate
export_tables "-post"
diff_tables
echo "success!"
