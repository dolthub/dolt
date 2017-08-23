#!/bin/sh
if [ "$#" -ne 1 ]; then
  echo "usage: $0 <docker-image-ref>"
  echo "runs this test on image matching <docker-image-ref>"
  exit 1
fi

# this tag is used by the dockerfiles in
# {data, server, client, bootstrap}
tag=zaqwsx_ipfs-test-img

# could use set -v, but i dont want to see the comments...

img=$(docker images | grep $1 | awk '{print $3}')
echo "using docker image: $img ($1)"

echo docker tag -f $img $tag
docker tag -f $img $tag

echo "fig build --no-cache"
fig build --no-cache

echo "fig up --no-color | tee build/fig.log"
fig up --no-color | tee build/fig.log

# save the ipfs logs for inspection
echo "make save_logs"
make save_logs || true # don't fail

# save the ipfs logs for inspection
echo "make save_profiling_data"
make save_profiling_data || true # don't fail

# fig up won't report the error using an error code, so we grep the
# fig.log file to find out whether the call succeeded
echo 'tail build/fig.log | grep "exited with code 0"'
tail build/fig.log | grep "exited with code 0"
