docker rm -f $( docker ps -q -a -f status=exited ) || true
