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
    c0 DECIMAL(65,30),
    c1 DECIMAL(65,30),
    c2 DECIMAL(65,30),
    c3 DECIMAL(65,30),
    c4 DECIMAL(65,30),
    c5 DECIMAL(65,30),
    c6 DECIMAL(65,30),
    c7 DECIMAL(65,30),
    c8 DECIMAL(65,30),
    c9 DECIMAL(65,30)
);]])

    print(string.format("Inserting %d records into 'tbl'", sysbench.opt.table_size))
    math.randomseed(0)
    con:bulk_insert_init("INSERT INTO tbl VALUES")
    for i = 1, sysbench.opt.table_size do
        row_values = "(" ..
        i .. "," ..              -- id
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c0
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c1
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c2
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c3
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c4
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c5
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c6
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c7
        math.random(-2147483648, 2147483647) + math.random() .. "," ..  -- c8
        math.random(-2147483648, 2147483647) + math.random() ..         -- c9
        ")"
        con:bulk_insert_next(row_values)
    end
    con:bulk_insert_done()
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    print("Dropping table 'tbl'")
    con:query("DROP TABLE IF EXISTS tbl")
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

