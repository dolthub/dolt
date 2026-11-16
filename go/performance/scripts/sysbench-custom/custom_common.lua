if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: prepare, run, cleanup, help")
end

sysbench.cmdline.options = {
    t1_size = {"size of table t1", 10},
    t2_size = {"size of table t2", 10000}
}

function init()
    assert(event ~= nil, "This script is not meant to be called directly")
end

function db_connection_init()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    return drv, con
end

function thread_init()
   drv, con = db_connection_init()
end

function thread_done()
   con:disconnect()
end

function cleanup()
    local drv, con = db_connection_init()
    con:query("DROP TABLE IF EXISTS t1, t2")
end

-- vim:ts=4 ss=4 sw=4 expandtab
