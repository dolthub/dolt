sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
}

chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

function rand_str()
    local length = math.random(1, 1024)
    local result = {}
    for i = 1, length do
        local idx = math.random(1, #chars)
        result[i] = chars:sub(idx, idx)
    end
    return table.concat(result)
end

function prepare()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    print("Creating table 'tbl'")
    con:query([[
CREATE TABLE tbl (
    id INT PRIMARY KEY,
    c0 TEXT,
    c1 TEXT,
    c2 TEXT,
    c3 TEXT,
    c4 TEXT,
    c5 TEXT,
    c6 TEXT,
    c7 TEXT,
    c8 TEXT,
    c9 TEXT
);]])

    print(string.format("Inserting %d records into 'tbl'", sysbench.opt.table_size))
    math.randomseed(0)
    con:bulk_insert_init("INSERT INTO tbl VALUES")
    for i = 1, sysbench.opt.table_size do
        row_values = "(" ..
        i .. "," ..                         -- id
        "'" .. rand_str() .. "'" .. "," ..  -- c0
        "'" .. rand_str() .. "'" .. "," ..  -- c1
        "'" .. rand_str() .. "'" .. "," ..  -- c2
        "'" .. rand_str() .. "'" .. "," ..  -- c3
        "'" .. rand_str() .. "'" .. "," ..  -- c4
        "'" .. rand_str() .. "'" .. "," ..  -- c5
        "'" .. rand_str() .. "'" .. "," ..  -- c6
        "'" .. rand_str() .. "'" .. "," ..  -- c7
        "'" .. rand_str() .. "'" .. "," ..  -- c8
        "'" .. rand_str() .. "'" ..         -- c9
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

