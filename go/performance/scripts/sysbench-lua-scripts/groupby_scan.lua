require("dolt_common")

dolt_prepare = prepare

function prepare()
    sysbench.opt.threads = 1
    dolt_prepare()
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('SELECT year_col, count(year_col), max(big_int_col), avg(small_int_col) FROM sbtest1 WHERE big_int_col > 0 GROUP BY set_col ORDER BY year_col')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end
