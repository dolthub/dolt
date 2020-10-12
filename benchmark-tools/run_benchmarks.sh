#!/bin/bash
set -e
set -o pipefail

[ ! -z "$1" ] || (echo "Please supply a comma separated list of tests to be run"; exit 1)
tests=$1
[ ! -z "$1" ] || (echo "Please supply a username to associate with the benchmark"; exit 1)
username=$2
committish_one=${3:-current}
committish_two=${4:-current}

if [ "$committish_one" == "$committish_two" ]; then
  echo "A single commit, $committish_one provided, proceeding with benchmark"
  committish_list="$committish_one"
else
  echo "Provided $committish_one and $committish_two, proceeding with building and benchmarking"
  committish_list="$committish_one $committish_two"
fi

script_dir=$(dirname "$0")
absolute_script_dir=$(realpath $script_dir)
working_dir="$absolute_script_dir/working"
echo "Ensuring $working_dir exists and is empty"
rm -rf $working_dir
mkdir $working_dir

function build_binary_at_committish() {
  build_committish=$0

  if [ "$build_commitish" != "current" ]; then
    echo "$build_commitish argument provided for 'commitish', cloning for fresh build"
    cd $working_dir
    git clone git@github.com:dolthub/dolt.git && git fetch --all && git checkout $build_commitish
    cd "dolt/go"
  else
    echo "$build_committish passed for committish arg, building from current repo"
    cd "$absolute_script_dir/../go"
  fi

  commit="$(git rev-parse HEAD)"
  if [[ $(git status --porcelain) ]]; then
    commit="$commit-dirty"
  fi

  echo "Commit is set to $commit"
  docker run --rm -v `pwd`:/src golang:1.14.2-buster /bin/bash -c '
    set -e
    set -o pipefail
    apt-get update && apt-get install -y zip
    cd /src

    o="out"
    mkdir -p "$o/bin"
    cp Godeps/LICENSES "$o/"
    echo Building "$o/dolt"
    obin="dolt"
    GOOS="$linux" GOARCH="$amd64" go build -o "$o/bin/$obin" "./cmd/dolt/"
  '
  echo "Moving binary to temp out/bin/dolt to $script_dir/working/$commit-dolt"

  mv "out/bin/dolt" "$absolute_script_dir/working/$commit-dolt"
  echo "$commit-dolt"
}


echo "Building binaries and benchmarking for $committish_list"
for committish in $committish_list; do
  echo "Building binary for $committish"
  bin_committish=$(build_binary_at_committish $committish)
  cd $absolute_script_dir
  echo "Built binary $bin_committish, executing benchmarks"
  docker run --rm -v `pwd`:/tools oscarbatori/dolt-sysbench /bin/bash -c '
    set -e
    set -o pipefail

    ln -s /tools/working/'$bin_committish' /usr/bin/dolt
    cd /tools

    dolt config --add --global user.name benchmark
    dolt config --add --global user.email benchmark

    python3 \
      sysbench_wrapper.py \
      --commitish='$committish' \
      --tests='$tests' \
      --username='$username'
  '
done

