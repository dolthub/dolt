#!/bin/sh

# Runtime is typically dominated by the costs of GetAttr (ie. Stat),
# so let's time that.  We use zipfs which runs from memory to minimize
# noise due to the filesystem itself.

if [ "$1" == "" ] ; then
  echo "Usage: benchmark.sh ZIPFILE"
  echo "The zipfile should be large (> 10000 files)."
  exit 2
fi

set -eux

ZIPFILE=$1
shift
CPU_COUNT=$(grep '^processor'  /proc/cpuinfo | wc -l)
export GOMAXPROCS=${CPU_COUNT}

DELAY=5

gomake -C zipfs
gomake -C bulkstat

MP=/tmp/zipbench
fusermount -u ${MP} || true
mkdir -p ${MP}

ZIPFS=$PWD/zipfs/zipfs
BULKSTAT="$PWD/bulkstat/bulkstat -threads ${CPU_COUNT}"

cd /tmp

${ZIPFS} ${MP} ${ZIPFILE} >& zipfs.log &


# Wait for FS to mount.
sleep ${DELAY}
find ${MP} > /tmp/zipfiles.txt
fusermount -u ${MP}

# Run vanilla: block box measurement.
${ZIPFS} ${MP} ${ZIPFILE} >& zipfs.log &

# Wait for zipfs to unpack and serve the file.
sleep ${DELAY}

# Performance number without 6prof running
echo -e "\n\n"
${BULKSTAT} -runs 5 /tmp/zipfiles.txt
echo -e "\n\n"

# Run 6prof
6prof -p $! -d 20 -t 3 -hs -l -h -f >& /tmp/zipfs.6prof &
sleep 0.1

# Feed data to 6prof
${BULKSTAT} -runs 3 /tmp/zipfiles.txt

echo -e "\n\n"

fusermount -u ${MP}

# Now run with internal monitoring.
${ZIPFS} -latencies ${MP} ${ZIPFILE} >& zipfs.log &

sleep ${DELAY}

# Measurements.
${BULKSTAT} -runs 5 /tmp/zipfiles.txt

# Dump internal measurements.
cat ${MP}/.debug/*



