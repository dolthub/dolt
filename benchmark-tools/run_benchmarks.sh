#!/bin/bash
set -e
set -o pipefail

[ -n "$1" ] || (echo "Please supply a comma separated list of tests to be run"; exit 1)
tests=$1
[ -n "$1" ] || (echo "Please supply a username to associate with the benchmark"; exit 1)
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
absolute_script_dir=$(realpath "$script_dir")
working_dir="$absolute_script_dir/dolt-builds/working"
echo "Ensuring $working_dir exists and is empty"
rm -rf "$working_dir"
mkdir "$working_dir"

function build_binary_at_committish() {
  build_committish=$1
  echo "Building binary for committish $build_committish"

  if [ "$build_committish" != "current" ]; then
    echo "$build_committish argument provided for 'commitish', cloning for fresh build"
    cd "$working_dir"
    git clone git@github.com:dolthub/dolt.git && git fetch --all
    cd "dolt/go"
    git checkout "$build_committish"
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
  echo "Moving binary to temp out/bin/dolt to $working_dir/$commit-dolt"
  mv "out/bin/dolt" "$working_dir/$commit-dolt"
  echo "$working_dir/$commit-dolt"
}

function run_sysbench() {
  subdir=$1
  env_vars_string=$2
  cd "$subdir"
  echo "Running docker-compose from $(pwd), with the following environment variables:"
  echo "$env_vars_string"
  docker-compose run $env_vars_string sysbench --build --rm --remove-orphans
  docker-compose down --remove-orphans
  cd ..
}

function get_commit_signature() {
  if [ "$1" == "current" ]; then
    if [[ $(git status --porcelain) ]]; then
      echo "$(git rev-parse HEAD)-dirty"
    else
      git rev-parse HEAD
    fi
  else
    echo "$1"
  fi
}

echo "Building binaries and benchmarking for $committish_list"
for committish in $committish_list; do
  bin_committish="$(build_binary_at_committish "$committish" | tail -1)"
  cd "$absolute_script_dir"
  echo "Built binary $bin_committish, copying to dolt-buidls/dolt for benchmarking"
  cp "$bin_committish" "$working_dir/dolt"
  run_sysbench dolt "-e DOLT_COMMITTISH=$(get_commit_signature $committish | tail -1) -e SYSBENCH_TESTS=$tests -e TEST_USERNAME=$username"
done

echo "Benchmarking MySQL for comparison"
run_sysbench mysql "-e SYSBENCH_TESTS=$tests -e TEST_USERNAME=$username"
echo "All done!"