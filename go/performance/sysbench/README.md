# Sysbench

MySQL tests expect local server (ex: `mysqld --port 3309 --local-infile=1 --socket=/tmp/mysqld2.sock`).

Interrupting a test midway can create cause "table already exists" error.