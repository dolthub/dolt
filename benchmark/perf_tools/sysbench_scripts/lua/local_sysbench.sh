sysbench --db-ps-mode=disable --rand-type=uniform --rand-seed=1 --percentile=50 --mysql-host=127.0.0.1 --mysql-user=root $@
