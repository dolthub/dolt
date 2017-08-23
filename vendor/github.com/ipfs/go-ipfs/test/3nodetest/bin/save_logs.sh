# STRIP strips color from terminal output
STRIP="perl -pe 's/\e\[?.*?[\@-~]//g'"

# TODO use a for loop like a grownup
docker logs 3nodetest_bootstrap_1 2>&1 | eval $STRIP > ./build/bootstrap.log
docker logs 3nodetest_client_1 2>&1 | eval $STRIP > ./build/client.log
docker logs 3nodetest_data_1 2>&1 | eval $STRIP > ./build/data.log
docker logs 3nodetest_server_1 2>&1 | eval $STRIP > ./build/server.log
