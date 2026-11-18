require("types_common_postgres")

dolt_prepare = prepare

function prepare()
    sysbench.opt.threads = 1
    dolt_prepare()
end

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function event()
    local id = sysbench.rand.default(1, 10000)
    con:query(string.format("DELETE FROM sbtest1 WHERE id = %d", id))
    local row_values = row_for_id(id)
    con:query(string.format("INSERT INTO sbtest1 VALUES %s", row_values))
end

