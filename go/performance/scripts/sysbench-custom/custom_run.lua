#!/usr/bin/env sysbench

require("custom_common")

function benchmark()
    con:query("SELECT * FROM t1 join t2 on i = j")
end

-- vim:ts=4 ss=4 sw=4 expandtab
