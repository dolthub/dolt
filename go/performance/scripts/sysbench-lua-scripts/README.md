# sysbench-lua-scripts

Making changes to scripts:

1. Run scripts locally by cloning the core Dolt repo, navigating to `go/performance/scripts`, and then running `local_sysbench.sh`. You can commit changes to feature branches in `scripts/sysbench-lua-scripts` (this repo), which will be cloned locally after running the local benchmark script once.

2. Edit the Dolt performance jobs to include new workflow scripts. `.github/scripts/performance-benchmarking/get-mysql-dolt-job-json.sh` and `.github/scripts/performance-benchmarking/get-dolt-dolt-job-json.sh` have `readTests` and `writeTests` variables that reference lua scripts by name. The remote worker will run every script name that is available in `sysbench-lua-scripts`.
