ipfs bootstrap add /ip4/$BOOTSTRAP_PORT_4011_TCP_ADDR/tcp/$BOOTSTRAP_PORT_4011_TCP_PORT/ipfs/QmNXuBh8HFsWq68Fid8dMbGNQTh7eG6hV9rr1fQyfmfomE
ipfs bootstrap # list bootstrap nodes for debugging

echo "3nodetest> starting client daemon"

ipfs daemon --debug &
sleep 3

# switch dirs so ipfs client profiling data doesn't overwrite the ipfs daemon
# profiling data
cd /tmp

while [ ! -f /data/idtiny ]
do
    echo "3nodetest> waiting for server to add the file..."
    sleep 1
done
echo "3nodetest> client found file with hash:" $(cat /data/idtiny)

ipfs cat $(cat /data/idtiny) > filetiny

cat filetiny

diff -u filetiny /data/filetiny

if (($? > 0)); then
    printf '%s\n' 'files did not match' >&2
    exit 1
fi

while [ ! -f /data/idrand ]
do
    echo "3nodetest> waiting for server to add the file..."
    sleep 1
done
echo "3nodetest> client found file with hash:" $(cat /data/idrand)

cat /data/idrand

ipfs cat $(cat /data/idrand) > filerand

if (($? > 0)); then
    printf '%s\n' 'ipfs cat failed' >&2
    exit 1
fi

diff -u filerand /data/filerand

if (($? > 0)); then
    printf '%s\n' 'files did not match' >&2
    exit 1
fi

echo "3nodetest> success"
