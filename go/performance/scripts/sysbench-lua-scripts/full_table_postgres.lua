sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
    create_table_options = {"Extra CREATE TABLE options", ""},
}

function prepare()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    print("Creating table 'sbtest1'")
    
    local create_query = string.format( [[
CREATE TABLE sbtest1 (
	id INT NOT NULL,
	col int not null,
	PRIMARY KEY(id)
); ]] .. sysbench.opt.create_table_options)

    con:query(create_query)

    if (sysbench.opt.table_size > 0) then
        print(string.format("Inserting %d records into 'sbtest1'", sysbench.opt.table_size))
    end

    local query = [[INSERT INTO sbtest1 (id, col) VALUES ]]

    local str_vals = {"val0", "val1", "val2"}
    math.randomseed(0)

    con:bulk_insert_init(query)
    for i = 1, sysbench.opt.table_size do
        local row_values = "(" .. i .. "," ..                   -- id
           math.random(-2147483648, 2147483647) .. ")"   -- col
        con:bulk_insert_next(row_values)
    end
    con:bulk_insert_done()

end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('SELECT * FROM sbtest1')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end
