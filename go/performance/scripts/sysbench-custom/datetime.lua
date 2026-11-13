sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
}

function rand_datetime()
    return string.format(
        "'%04d-%02d-%02d %02d:%02d:%02d.%06d'",
        math.random(0, 9999),  -- year
        math.random(1, 12),    -- month
        math.random(1, 28),    -- day
        math.random(0, 23),    -- hours
        math.random(0, 59),    -- mins
        math.random(0, 59),    -- secs
        math.random(0, 999999) -- micros
    )
end

function prepare()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    print("Creating table 'tbl'")
    con:query([[
CREATE TABLE tbl (
    id INT PRIMARY KEY,
    c0 DATETIME(6),
    c1 DATETIME(6),
    c2 DATETIME(6),
    c3 DATETIME(6),
    c4 DATETIME(6),
    c5 DATETIME(6),
    c6 DATETIME(6),
    c7 DATETIME(6),
    c8 DATETIME(6),
    c9 DATETIME(6)
);]])

    print(string.format("Inserting %d records into 'tbl'", sysbench.opt.table_size))
    math.randomseed(0)
    con:bulk_insert_init("INSERT INTO tbl VALUES")
    for i = 1, sysbench.opt.table_size do
        row_values = "(" ..
        i .. "," ..                -- id
        rand_datetime() .. "," ..  -- c0
        rand_datetime() .. "," ..  -- c1
        rand_datetime() .. "," ..  -- c2
        rand_datetime() .. "," ..  -- c3
        rand_datetime() .. "," ..  -- c4
        rand_datetime() .. "," ..  -- c5
        rand_datetime() .. "," ..  -- c6
        rand_datetime() .. "," ..  -- c7
        rand_datetime() .. "," ..  -- c8
        rand_datetime() ..         -- c9
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

