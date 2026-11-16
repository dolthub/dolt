#!/usr/bin/env sysbench

require("custom_common")
require("custom_run")

function create_tables(drv, con)
    print("Creating tables...")
    print(string.format("t1_size = %d", sysbench.opt.t1_size))
    print(string.format("t2_size = %d", sysbench.opt.t2_size))

    con:query("CREATE TABLE t1 (i INT PRIMARY KEY)")
    con:bulk_insert_init("INSERT INTO t1 VALUES")
    for i = 1, sysbench.opt.t1_size do
        query = string.format([[(%d)]], i)
        con:bulk_insert_next(query)
    end
    con:bulk_insert_done()

    con:query("CREATE TABLE t2 (j INT PRIMARY KEY)")
    con:bulk_insert_init("INSERT INTO t2 VALUES")
    for i = 1, sysbench.opt.t2_size do
        query = string.format([[(%d)]], i)
        con:bulk_insert_next(query)
    end
    con:bulk_insert_done()
end

function cmd_prepare()
    local drv, con = db_connection_init()
    create_tables(drv, con)
end

sysbench.cmdline.commands = {
    prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND}
}

function event()
    -- Execute transaction
    _G["benchmark"]()
end

-- vim:ts=4 ss=4 sw=4 expandtab
