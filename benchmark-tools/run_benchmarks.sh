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
working_dir="$absolute_script_dir/working"
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
  rm -rf "$absolute_script_dir/dolt-builds/working"
  mkdir -p "$absolute_script_dir/dolt-builds/working"
  echo "Moving binary to temp out/bin/dolt to $absolute_script_dir/dolt-builds/working/$commit-dolt"
  mv "out/bin/dolt" "$absolute_script_dir/dolt-builds/working/$commit-dolt"
}

# Set environment variables to be picked up by docker-compose
SYSBENCH_TEST=$tests
TEST_USERNAME=$username

echo "Building binaries and benchmarking for $committish_list"
for committish in $committish_list; do
  DOLT_COMMITTISH=$committish
  build_binary_at_committish "$committish"
  cd "$absolute_script_dir"
  if [ "$committish" != "current" ]; then
    bin_committish="$committish-dolt"
  else
    cur_commit=$(git rev-parse HEAD)
    if [[ $(git status --porcelain) ]]; then
      bin_committish="$cur_commit-dirty-dolt"
      committish="$cur_commit"
    else
      bin_committish="$cur_commit-dolt"
      committish="$cur_commit"
    fi
  fi
  echo "Built binary $bin_committish, moving to  dolt-builds/dolt"
  mv "$absolute_script_dir/dolt-builds/working/$bin_committish" "$absolute_script_dir/dolt-builds/dolt"

  cd dolt
  cat <<EOF >> $absolute_script_dir/working/dolt.env
DOLT_COMMITTISH=$DOLT_COMMITTISH
SYSBENCH_TEST=$SYSBENCH_TEST
TEST_USERNAME=$TEST_USERNAME
EOF
  cat $absolute_script_dir/working/dolt.env
  docker-compose --env-file $absolute_script_dir/working/dolt.env up

done

echo "Running benchmarks for MySQL for comparison"
cd mysql
docker-compose run -e SYSBENCH_TEST,TEST_USERNAME