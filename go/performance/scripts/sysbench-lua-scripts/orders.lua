sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
    create_table_options = {"Extra CREATE TABLE options", ""},
}

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
    con:query("SET autocommit=0")

    stmt = con:prepare('SELECT no_o_id FROM new_orders1 WHERE no_d_id = 1 AND no_w_id = 1 ORDER BY no_o_id ASC LIMIT 1;')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    con:query("")
    stmt:execute()
end
