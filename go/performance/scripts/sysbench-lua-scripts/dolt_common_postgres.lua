
-- -----------------------------------------------------------------------------
-- Common code for dolt benchmarks.
-- -----------------------------------------------------------------------------

function init()
    assert(event ~= nil, "this script is meant to be included by other OLTP scripts and should not be called directly.")
end

if sysbench.cmdline.command == nil then
    error("Command is required. Supported commands: prepare, warmup, run, cleanup, help")
end

-- Command line options
sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
    create_table_options = {"Extra CREATE TABLE options", ""},
}

-- Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --tables > 1
function cmd_prepare()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    print("Creating table 'sbtest1'")

    -- postgres doesn't have unsigned integer types, but to keep the performance comparison as close
    -- to MySQL as possible, we include signed versions of those columns
    -- https://github.com/dolthub/doltgresql/issues/448
    local create_query = string.format( [[
CREATE TABLE sbtest1 (
    id INT NOT NULL PRIMARY KEY,
    tiny_int_col SMALLINT NOT NULL,
    unsigned_tiny_int_col SMALLINT NOT NULL,
    small_int_col SMALLINT NOT NULL,
    unsigned_small_int_col SMALLINT NOT NULL,
    medium_int_col INTEGER NOT NULL,
    unsigned_medium_int_col INTEGER NOT NULL,
    int_col INTEGER NOT NULL,
    unsigned_int_col INTEGER NOT NULL,
    big_int_col BIGINT NOT NULL,
    unsigned_big_int_col BIGINT NOT NULL,
    decimal_col DECIMAL NOT NULL,
    float_col REAL NOT NULL,
    double_col DOUBLE PRECISION NOT NULL,
--    bit_col BIT NOT NULL,
    bit_col VARCHAR(64) NOT NULL,
    char_col CHAR NOT NULL,
    var_char_col VARCHAR(64) NOT NULL,
    enum_col VARCHAR(5), -- CHECK (enum_col IN ('val0', 'val1', 'val2')) NOT NULL,
    set_col VARCHAR(5), -- CHECK (set_col IN ('val0', 'val1', 'val2')) NOT NULL,
    date_col DATE NOT NULL,
    time_col TIME NOT NULL,
    datetime_col TIMESTAMP NOT NULL,
    timestamp_col TIMESTAMP NOT NULL,
    year_col SMALLINT NOT NULL
); ]] .. sysbench.opt.create_table_options)

    con:query(create_query)

    con:query("CREATE INDEX idx_big_int_col ON sbtest1(big_int_col);")

    print("Creating table 'sbtest2'")

    local create_query = string.format( [[
CREATE TABLE sbtest2 (
    id INT NOT NULL,
    int_col INT NOT NULL,
    unsigned_int_col INT  NOT NULL,
    char_col CHAR NOT NULL,
    var_char_col VARCHAR(64) NOT NULL,
    PRIMARY KEY(id)
); ]] .. sysbench.opt.create_table_options)

    con:query(create_query)

    if (sysbench.opt.table_size > 0) then
        print(string.format("Inserting %d records into 'sbtest1'", sysbench.opt.table_size))
    end

    local query = [[INSERT INTO sbtest1
(id, tiny_int_col, unsigned_tiny_int_col, small_int_col, unsigned_small_int_col, medium_int_col, unsigned_medium_int_col, int_col, unsigned_int_col, big_int_col, unsigned_big_int_col, decimal_col, float_col, double_col, bit_col, char_col, var_char_col, enum_col, set_col, date_col, time_col, datetime_col, timestamp_col, year_col)
VALUES
]]

    local str_vals = {"val0", "val1", "val2"}
    math.randomseed(0)

    con:bulk_insert_init(query)
    for i = 1, sysbench.opt.table_size do
        local row_values = "(" .. i .. "," ..                                             -- id
            math.random(-128, 127) .. "," ..                                   -- tiny_int_col
            math.random(-128, 127) .. "," ..                                   -- unsigned_tiny_int_col
            math.random(-32768, 32767) .. "," ..                               -- small_int_col
            math.random(-32768, 32767) .. "," ..                                    -- unsigned_small_int_col
            math.random(-8388608, 8388607) .. "," ..                           -- medium_int_col
            math.random(-8388608, 8388607) .. "," ..                                 -- unsigned_medium_int_col
            math.random(-2147483648, 2147483647) .. "," ..                     -- int_col
            math.random(-2147483648, 2147483647) .. "," ..                               -- unsigned_int_col
            math.random(-4611686018427387904, 4611686018427387903) .. "," ..   -- big_int_col
            math.random(-4611686018427387904, 4611686018427387903) .. "," ..                      -- unsigned_big_int_col
            math.random() .. "," ..                                                    -- decimal_col
            math.random() .. "," ..                                                    -- float_col
            math.random() .. "," ..                                                    -- double_col
            "'" .. math.random(0, 1) .. "'," ..                                        -- bit_col
            "'" .. string.char(math.random(0x30, 0x5A)) .. "'" .. "," ..          -- char_col
            "'" .. str_vals[math.random(1, 3)] .. "'" .. "," ..                -- var_char_col
            "'" .. str_vals[math.random(1, 3)] .. "'" .. "," ..                -- enum_col
            "'" .. str_vals[math.random(1, 3)] .. "'" .. "," ..                -- set_col
            "'2020-0" .. math.random(1, 9) .. "-" .. math.random(10, 28) .. "'" .. "," .. -- date_col
            "'0" .. math.random(1, 9) .. ":" .. math.random(10, 59) .. ":00'" .. "," .. -- time_col
            "'2020-0" .. math.random(1, 9) .. "-" .. math.random(10, 28) .. " 0" .. math.random(1, 9) .. ":" .. math.random(10, 59) .. ":00'" .. "," .. -- datetime_col
            "'2020-0" .. math.random(1, 9) .. "-" .. math.random(10, 28) .. " 0" .. math.random(1, 9) .. ":" .. math.random(10, 59) .. ":00'" .. "," .. -- timestamp_col
            math.random(1901, 2155) .. ")"                                     -- year_col

        con:bulk_insert_next(row_values)
    end
    con:bulk_insert_done()

    if (sysbench.opt.table_size > 0) then
        print(string.format("Inserting %d records into 'sbtest2'", sysbench.opt.table_size))
    end

    local query = [[INSERT INTO sbtest2 (id, int_col, unsigned_int_col, char_col, var_char_col) VALUES ]]

    local str_vals = {"val0", "val1", "val2"}
    math.randomseed(0)

    con:bulk_insert_init(query)
    for i = 1, sysbench.opt.table_size do
        local row_values = "(" .. i .. "," ..                             -- id
            math.random(0, 10000) .. "," ..                               -- int_col (value range matches sbtest1.id)
            math.random(-2147483648, 2147483647) .. "," ..                          -- unsigned_int_col
            "'" .. string.char(math.random(0x30, 0x5A)) .. "'" .. "," ..  -- char_col
            "'" .. str_vals[math.random(1, 3)] .. "'" .. ")"              -- var_char_col
        con:bulk_insert_next(row_values)
    end
    con:bulk_insert_done()

end


-- Implement parallel prepare and warmup commands, define 'prewarm' as an alias
-- for 'warmup'
sysbench.cmdline.commands = {
    prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
}

local t = sysbench.sql.type

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()

    print("Dropping table 'sbtest1'")
    con:query("DROP TABLE IF EXISTS sbtest1")
    print("Dropping table 'sbtest2'")
    con:query("DROP TABLE IF EXISTS sbtest2")
end



