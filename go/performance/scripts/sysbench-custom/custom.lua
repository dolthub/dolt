sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
}

function prepare()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    print("Creating table 'tbl'")
    con:query([[
CREATE TABLE tbl (
    id INT PRIMARY KEY,
    c0 INT,
    c1 INT,
    c2 INT,
    c3 INT,
    c4 INT,
    c5 INT,
    c6 INT,
    c7 INT,
    c8 INT,
    c9 INT
);]])

    print(string.format("Inserting %d records into 'tbl'", sysbench.opt.tbl_size))
    math.randomseed(0)
    con:bulk_insert_init("INSERT INTO tbl VALUES")
    for i = 1, sysbench.opt.table_size do
        row_values = "(" ..
        i .. "," ..                                     -- id
        math.random(-2147483648, 2147483647) .. "," ..  -- c0
        math.random(-2147483648, 2147483647) .. "," ..  -- c1
        math.random(-2147483648, 2147483647) .. "," ..  -- c2
        math.random(-2147483648, 2147483647) .. "," ..  -- c3
        math.random(-2147483648, 2147483647) .. "," ..  -- c4
        math.random(-2147483648, 2147483647) .. "," ..  -- c5
        math.random(-2147483648, 2147483647) .. "," ..  -- c6
        math.random(-2147483648, 2147483647) .. "," ..  -- c7
        math.random(-2147483648, 2147483647) .. "," ..  -- c8
        math.random(-2147483648, 2147483647) ..         -- c9
        ")"
        con:bulk_insert_next(row_values)
    end
    con:bulk_insert_done()
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('SELECT * FROM tbl')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end

