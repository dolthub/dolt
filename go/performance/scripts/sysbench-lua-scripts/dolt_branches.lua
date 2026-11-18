
require("dolt_common")

dolt_prepare = prepare

function prepare()
    sysbench.opt.threads = 1
    dolt_prepare()
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('SELECT @@replica_head')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end
