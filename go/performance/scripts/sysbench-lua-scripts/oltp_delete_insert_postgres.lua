-- Copyright (C) 2006-2018 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

function prepare()
    sysbench.opt.threads = 1
    drv = sysbench.sql.driver()
    con = drv:connect()
    create_table(drv, con, 1)
end

function thread_init()
  drv = sysbench.sql.driver()
  con = drv:connect()
end

function thread_done()
    con:disconnect()
end

function event()
    local id = sysbench.rand.default(1, 10000)
    local k_val = sysbench.rand.default(1, 10000)
    local c_val = get_c_value()
    local pad_val = get_pad_value()

    con:query(string.format("DELETE FROM sbtest1 WHERE id = %d", id))
    con:query(string.format("INSERT INTO sbtest1 (id, k, c, pad) VALUES " ..
                            "(%d, %d, '%s', '%s')", id, k_val, c_val, pad_val))
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    print("Dropping table 'sbtest1'")
    con:query("DROP TABLE IF EXISTS sbtest1")
end


-- 10 groups, 119 characters
local c_value_template = "###########-###########-###########-" ..
   "###########-###########-###########-" ..
   "###########-###########-###########-" ..
   "###########"

-- 5 groups, 59 characters
local pad_value_template = "###########-###########-###########-" ..
   "###########-###########"

function get_c_value()
   return sysbench.rand.string(c_value_template)
end

function get_pad_value()
   return sysbench.rand.string(pad_value_template)
end

function create_table(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query

   print(string.format("Creating table 'sbtest%d'...", table_num))

   -- MySQL has auto_increment for the ID column
   query = [[
CREATE TABLE sbtest1 (
  id int NOT NULL,
  k INTEGER DEFAULT '0' NOT NULL,
  c CHAR(120) DEFAULT '' NOT NULL,
  pad CHAR(60) DEFAULT '' NOT NULL,
  PRIMARY KEY (id)
) ]]

   con:query(query)
   con:query("create index k_1 on sbtest1(k)")
   
   query = "INSERT INTO sbtest" .. table_num .. "(id, k, c, pad) VALUES"
   con:bulk_insert_init(query)

   local c_val
   local pad_val

   for i = 1, 10000 do
      c_val = get_c_value()
      pad_val = get_pad_value()
      query = string.format("(%d, %d, '%s', '%s')",
                                     i, sb_rand(1, 10000), c_val,
                                     pad_val)
      con:bulk_insert_next(query)
   end

   con:bulk_insert_done()
end
