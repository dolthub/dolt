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
    c0 FLOAT,
    c1 FLOAT,
    c2 FLOAT,
    c3 FLOAT,
    c4 FLOAT,
    c5 FLOAT,
    c6 FLOAT,
    c7 FLOAT,
    c8 FLOAT,
    c9 FLOAT
);]])

    print(string.format("Inserting %d records into 'tbl'", sysbench.opt.table_size))
    math.randomseed(0)
    con:bulk_insert_init("INSERT INTO tbl VALUES")
    for i = 1, sysbench.opt.table_size do
        row_values = "(" ..
        i .. "," ..              -- id
        math.random() .. "," ..  -- c0
        math.random() .. "," ..  -- c1
        math.random() .. "," ..  -- c2
        math.random() .. "," ..  -- c3
        math.random() .. "," ..  -- c4
        math.random() .. "," ..  -- c5
        math.random() .. "," ..  -- c6
        math.random() .. "," ..  -- c7
        math.random() .. "," ..  -- c8
        math.random() ..         -- c9
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

