-- Copyright (C) 2006-2017 Vadim Tkachenko, Percona

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

-- -----------------------------------------------------------------------------
-- Common code for TPCC benchmarks.
-- -----------------------------------------------------------------------------

ffi = require("ffi")

ffi.cdef[[
void sb_counter_inc(int, sb_counter_type);
typedef uint32_t useconds_t;
int usleep(useconds_t useconds);
]]


function init()
   assert(event ~= nil,
          "this script is meant to be included by other TPCC scripts and " ..
             "should not be called directly.")
end

if sysbench.cmdline.command == nil then
   error("Command is required. Supported commands: prepare, run, cleanup, help")
end

MAXITEMS=100000
DIST_PER_WARE=10
CUST_PER_DIST=3000

-- Command line options
sysbench.cmdline.options = {
   scale =
      {"Scale factor (warehouses)", 100},
   tables =
      {"Number of tables", 1},
   use_fk =
      {"Use foreign keys", 1},
   force_pk =
      {"Force using auto-inc PK on history table", 0},
   trx_level =
      {"Transaction isolation level (RC, RR or SER)", "RR"},
   enable_purge =
      {"Use purge transaction (yes, no)", "no"},
   report_csv =
      {"Report output in csv (yes, no)", "no"},
   pgsql_schema =
      {"Schema name for Pg(default:public)", "public"},
   mysql_storage_engine =
      {"Storage engine, if MySQL is used", "innodb"},
   mysql_table_options =
      {"Extra table options, if MySQL is used. e.g. 'COLLATE latin1_bin'", ""},
   splittable =
      {"Create READ WRITE or READ ONLY transactions to allow using a splitting proxy", "no"}
}

function sleep(n)
  os.execute("sleep " .. tonumber(n))
end

function db_connection_init()
   local drv = sysbench.sql.driver()
   local con = drv:connect()

   set_isolation_level(drv,con)

   if drv:name() == "mysql" then 
      con:query("SET FOREIGN_KEY_CHECKS=0")
      con:query("SET autocommit=0")
   end

   if drv:name() == "pgsql" then
     con:query("SET search_path TO " .. sysbench.opt.pgsql_schema)
     print ("DB SCHEMA ".. sysbench.opt.pgsql_schema)
   end

   return drv,con
end

-- Create the tables and Prepare the dataset. This command supports parallel execution, i.e. will
-- benefit from executing with --threads > 1 as long as --scale > 1
function cmd_prepare()

   local drv,con = db_connection_init()

   -- create tables in parallel table per thread
   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.tables,
   sysbench.opt.threads do
     create_tables(drv, con, i)
   end

   -- make sure all tables are created before we load data

   print("Waiting on tables 30 sec\n")
   sleep(30)

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.scale,
   sysbench.opt.threads do
     load_tables(drv, con, i)
   end

end

-- Check consistency 
-- benefit from executing with --threads > 1 as long as --scale > 1
function cmd_check()

   local drv,con = db_connection_init()

   for i = sysbench.tid % sysbench.opt.threads + 1, sysbench.opt.scale,
   sysbench.opt.threads do
     check_tables(drv, con, i)
   end

end

-- Implement parallel prepare and prewarm commands
sysbench.cmdline.commands = {
   prepare = {cmd_prepare, sysbench.cmdline.PARALLEL_COMMAND},
   check = {cmd_check, sysbench.cmdline.PARALLEL_COMMAND}
}


function create_tables(drv, con, table_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query
   local tinyint_type="smallint"
   local datetime_type="timestamp"   
   
   if drv:name() == "mysql" or drv:name() == "attachsql" or
      drv:name() == "drizzle"
   then
      engine_def = "/*! ENGINE = " .. sysbench.opt.mysql_storage_engine .. " */"
      extra_table_options = sysbench.opt.mysql_table_options or ""
      tinyint_type="tinyint"
      datetime_type="datetime"
   end

   print(string.format("Creating tables: %d\n", table_num))

   query = string.format([[
	CREATE TABLE IF NOT EXISTS warehouse%d (
	w_id smallint not null,
	w_name varchar(10), 
	w_street_1 varchar(20), 
	w_street_2 varchar(20), 
	w_city varchar(20), 
	w_state char(2), 
	w_zip char(9), 
	w_tax decimal(4,2), 
	w_ytd decimal(12,2),
	primary key (w_id) 
	) %s %s]],
        table_num, engine_def, extra_table_options)

   con:query(query)

   query = string.format([[
	create table IF NOT EXISTS district%d (
	d_id ]] .. tinyint_type .. [[ not null, 
	d_w_id smallint not null, 
	d_name varchar(10), 
	d_street_1 varchar(20), 
	d_street_2 varchar(20), 
	d_city varchar(20), 
	d_state char(2), 
	d_zip char(9), 
	d_tax decimal(4,2), 
	d_ytd decimal(12,2), 
	d_next_o_id int,
	primary key (d_w_id, d_id) 
	) %s %s]],
      table_num, engine_def, extra_table_options)

    con:query(query)

-- CUSTOMER TABLE

   query = string.format([[
	create table IF NOT EXISTS customer%d (
	c_id int not null, 
	c_d_id ]] .. tinyint_type .. [[ not null,
	c_w_id smallint not null, 
	c_first varchar(16), 
	c_middle char(2), 
	c_last varchar(16), 
	c_street_1 varchar(20), 
	c_street_2 varchar(20), 
	c_city varchar(20), 
	c_state char(2), 
	c_zip char(9), 
	c_phone char(16), 
	c_since ]] .. datetime_type .. [[, 
	c_credit char(2), 
	c_credit_lim bigint, 
	c_discount decimal(4,2), 
	c_balance decimal(12,2), 
	c_ytd_payment decimal(12,2), 
	c_payment_cnt smallint, 
	c_delivery_cnt smallint, 
	c_data text,
	PRIMARY KEY(c_w_id, c_d_id, c_id)
	) %s %s]],
      table_num, engine_def, extra_table_options)

   con:query(query)

-- HISTORY TABLE
   local hist_auto_inc=""
   local hist_pk=""
   if sysbench.opt.force_pk == 1 then
      hist_auto_inc="id int NOT NULL AUTO_INCREMENT,"
      hist_pk=",PRIMARY KEY(id)"
   end
   query = string.format([[
	create table IF NOT EXISTS history%d (
        %s
	h_c_id int, 
	h_c_d_id ]] .. tinyint_type .. [[, 
	h_c_w_id smallint,
	h_d_id ]] .. tinyint_type .. [[,
	h_w_id smallint,
	h_date ]] .. datetime_type .. [[,
	h_amount decimal(6,2), 
	h_data varchar(24) %s
	) %s %s]],
      table_num, hist_auto_inc, hist_pk, engine_def, extra_table_options)

   con:query(query)

   query = string.format([[
	create table IF NOT EXISTS orders%d (
	o_id int not null, 
	o_d_id ]] .. tinyint_type .. [[ not null, 
	o_w_id smallint not null,
	o_c_id int,
	o_entry_d ]] .. datetime_type .. [[,
	o_carrier_id ]] .. tinyint_type .. [[,
	o_ol_cnt ]] .. tinyint_type .. [[, 
	o_all_local ]] .. tinyint_type .. [[,
	PRIMARY KEY(o_w_id, o_d_id, o_id) 
	) %s %s]],
      table_num, engine_def, extra_table_options)

   con:query(query)

-- NEW_ORDER table

   query = string.format([[
	create table IF NOT EXISTS new_orders%d (
	no_o_id int not null,
	no_d_id ]] .. tinyint_type .. [[ not null,
	no_w_id smallint not null,
	PRIMARY KEY(no_w_id, no_d_id, no_o_id)
	) %s %s]],
      table_num, engine_def, extra_table_options)

   con:query(query)

   query = string.format([[
	create table IF NOT EXISTS order_line%d ( 
	ol_o_id int not null, 
	ol_d_id ]] .. tinyint_type .. [[ not null,
	ol_w_id smallint not null,
	ol_number ]] .. tinyint_type .. [[ not null,
	ol_i_id int, 
	ol_supply_w_id smallint,
	ol_delivery_d ]] .. datetime_type .. [[, 
	ol_quantity ]] .. tinyint_type .. [[, 
	ol_amount decimal(6,2), 
	ol_dist_info char(24),
	PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number)
	) %s %s]],
      table_num, engine_def, extra_table_options)

   con:query(query)

-- STOCK table

   query = string.format([[
	create table IF NOT EXISTS stock%d (
	s_i_id int not null, 
	s_w_id smallint not null, 
	s_quantity smallint, 
	s_dist_01 char(24), 
	s_dist_02 char(24),
	s_dist_03 char(24),
	s_dist_04 char(24), 
	s_dist_05 char(24), 
	s_dist_06 char(24), 
	s_dist_07 char(24), 
	s_dist_08 char(24), 
	s_dist_09 char(24), 
	s_dist_10 char(24), 
	s_ytd decimal(8,0), 
	s_order_cnt smallint, 
	s_remote_cnt smallint,
	s_data varchar(50),
	PRIMARY KEY(s_w_id, s_i_id)
	) %s %s]],
      table_num, engine_def, extra_table_options)

   con:query(query)

   local i = table_num

   query = string.format([[
	create table IF NOT EXISTS item%d (
	i_id int not null, 
	i_im_id int, 
	i_name varchar(24), 
	i_price decimal(5,2), 
	i_data varchar(50),
	PRIMARY KEY(i_id) 
	) %s %s]],
      i, engine_def, extra_table_options)

   con:query(query)

   con:bulk_insert_init("INSERT INTO item" .. i .." (i_id, i_im_id, i_name, i_price, i_data) values")
   for j = 1 , MAXITEMS do
      local i_im_id = sysbench.rand.uniform(1,10000)
      local i_price = sysbench.rand.uniform_double()*100+1
      -- i_name is not generated as prescribed by standard, but we want to provide a better compression
      local i_name  = string.format("item-%d-%f-%s", i_im_id, i_price, sysbench.rand.string("@@@@@"))
      local i_data  = string.format("data-%s-%s", i_name, sysbench.rand.string("@@@@@"))
 
      query = string.format([[(%d,%d,'%s',%f,'%s')]],
	j, i_im_id, i_name:sub(1,24), i_price, i_data:sub(1,50))
        con:bulk_insert_next(query)
		 
   end
   con:bulk_insert_done()

    print(string.format("Adding indexes %d ... \n", i))
    con:query("CREATE INDEX idx_customer"..i.." ON customer"..i.." (c_w_id,c_d_id,c_last,c_first)")
    con:query("CREATE INDEX idx_orders"..i.." ON orders"..i.." (o_w_id,o_d_id,o_c_id,o_id)")
    con:query("CREATE INDEX fkey_stock_2"..i.." ON stock"..i.." (s_i_id)")
    con:query("CREATE INDEX fkey_order_line_2"..i.." ON order_line"..i.." (ol_supply_w_id,ol_i_id)")
    con:query("CREATE INDEX fkey_history_1"..i.." ON history"..i.." (h_c_w_id,h_c_d_id,h_c_id)")
    con:query("CREATE INDEX fkey_history_2"..i.." ON history"..i.." (h_w_id,h_d_id )")
    if sysbench.opt.use_fk == 1 then
        print(string.format("Adding FK %d ... \n", i))
        con:query("ALTER TABLE new_orders"..i.." ADD CONSTRAINT fkey_new_orders_1_"..table_num.." FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES orders"..i.."(o_w_id,o_d_id,o_id)")
        con:query("ALTER TABLE orders"..i.." ADD CONSTRAINT fkey_orders_1_"..table_num.." FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer"..i.."(c_w_id,c_d_id,c_id)")
        con:query("ALTER TABLE customer"..i.." ADD CONSTRAINT fkey_customer_1_"..table_num.." FOREIGN KEY(c_w_id,c_d_id) REFERENCES district"..i.."(d_w_id,d_id)")
        con:query("ALTER TABLE history"..i.." ADD CONSTRAINT fkey_history_1_"..table_num.." FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer"..i.."(c_w_id,c_d_id,c_id)")
        con:query("ALTER TABLE history"..i.." ADD CONSTRAINT fkey_history_2_"..table_num.." FOREIGN KEY(h_w_id,h_d_id) REFERENCES district"..i.."(d_w_id,d_id)")
        con:query("ALTER TABLE district"..i.." ADD CONSTRAINT fkey_district_1_"..table_num.." FOREIGN KEY(d_w_id) REFERENCES warehouse"..i.."(w_id)")
        con:query("ALTER TABLE order_line"..i.." ADD CONSTRAINT fkey_order_line_1_"..table_num.." FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES orders"..i.."(o_w_id,o_d_id,o_id)")
        con:query("ALTER TABLE order_line"..i.." ADD CONSTRAINT fkey_order_line_2_"..table_num.." FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock"..i.."(s_w_id,s_i_id)")
        con:query("ALTER TABLE stock"..i.." ADD CONSTRAINT fkey_stock_1_"..table_num.." FOREIGN KEY(s_w_id) REFERENCES warehouse"..i.."(w_id)")
        con:query("ALTER TABLE stock"..i.." ADD CONSTRAINT fkey_stock_2_"..table_num.." FOREIGN KEY(s_i_id) REFERENCES item"..i.."(i_id)")
    end
end


function set_isolation_level(drv,con)
   if drv:name() == "mysql"
   then
        if sysbench.opt.trx_level == "RR" then
            isolation_level="REPEATABLE-READ"
        elseif sysbench.opt.trx_level == "RC" then
            isolation_level="READ-COMMITTED"
        elseif sysbench.opt.trx_level == "SER" then
            isolation_level="SERIALIZABLE"
        end
       
        isolation_variable=con:query_row("SHOW VARIABLES LIKE 't%_isolation'")

        con:query("SET SESSION " .. isolation_variable .. "='".. isolation_level .."'")
   end

   if drv:name() == "pgsql"
   then
        if sysbench.opt.trx_level == "RR" then
            isolation_level="REPEATABLE READ"
        elseif sysbench.opt.trx_level == "RC" then
            isolation_level="READ COMMITTED"
        elseif sysbench.opt.trx_level == "SER" then
            isolation_level="SERIALIZABLE"
        end
       
        con:query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL " .. isolation_level )
   end

end



function load_tables(drv, con, warehouse_num)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query


   -- print(string.format("Creating warehouse: %d\n", warehouse_num))
   if drv:name() == "mysql"
   then
   	con:query("SET SESSION autocommit=1")
	-- con:query("SET SESSION sql_log_bin = 0")
   	-- con:query("SET @trx = (SELECT @@global.innodb_flush_log_at_trx_commit)")
   	-- con:query("SET GLOBAL innodb_flush_log_at_trx_commit=0")
   end

   for table_num = 1, sysbench.opt.tables do 
	  --con:query("SET autocommit=1")

   print(string.format("loading tables: %d for warehouse: %d\n", table_num, warehouse_num))

    con:bulk_insert_init("INSERT INTO warehouse" .. table_num .. 
	" (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) values")

    query = string.format([[(%d, '%s','%s','%s', '%s', '%s', '%s', %f,300000)]],
	warehouse_num, sysbench.rand.string("name-@@@@@"), sysbench.rand.string("street1-@@@@@@@@@@"),
        sysbench.rand.string("street2-@@@@@@@@@@"), sysbench.rand.string("city-@@@@@@@@@@"),
        sysbench.rand.string("@@"),sysbench.rand.string("zip-#####"),sysbench.rand.uniform_double()*0.2 )

    con:bulk_insert_next(query)
    con:bulk_insert_done()

    con:bulk_insert_init("INSERT INTO district" .. table_num .. 
	" (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) values")

   for d_id = 1 , DIST_PER_WARE do
 
      query = string.format([[(%d, %d, '%s','%s','%s', '%s', '%s', '%s', %f,30000,3001)]],
	d_id, warehouse_num, sysbench.rand.string("name-@@@@@"), sysbench.rand.string("street1-@@@@@@@@@@"),
        sysbench.rand.string("street2-@@@@@@@@@@"), sysbench.rand.string("city-@@@@@@@@@@"),
        sysbench.rand.string("@@"),sysbench.rand.string("zip-#####"),sysbench.rand.uniform_double()*0.2 )
      con:bulk_insert_next(query)

   end
   con:bulk_insert_done()

-- CUSTOMER TABLE

   con:bulk_insert_init("INSERT INTO customer" .. table_num .. [[
	  (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, 
	   c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, 
           c_data) values]])

   for d_id = 1 , DIST_PER_WARE do
   for c_id = 1 , CUST_PER_DIST do
        local c_last

	if c_id <= 1000 then
		c_last = Lastname(c_id - 1)
	else 
		c_last = Lastname(NURand(255, 0, 999))
	end
 
      query = string.format([[(%d, %d, %d, '%s','OE','%s','%s', '%s', '%s', '%s', '%s','%s',NOW(),'%s',50000,%f,-10,10,1,0,'%s' )]],
	c_id, d_id, warehouse_num, 
        sysbench.rand.string("first-"..string.rep("@",sysbench.rand.uniform(2,10))), 
        c_last,
        sysbench.rand.string("street1-@@@@@@@@@@"),
        sysbench.rand.string("street2-@@@@@@@@@@"), sysbench.rand.string("city-@@@@@@@@@@"),
        sysbench.rand.string("@@"),sysbench.rand.string("zip-#####"),
        sysbench.rand.string(string.rep("#",16)),
        (sysbench.rand.uniform(1,100) > 10) and "GC" or "BC",
	sysbench.rand.uniform_double()*0.5,
	string.rep(sysbench.rand.string("@"),sysbench.rand.uniform(300,500))
        
        )
      con:bulk_insert_next(query)

   end 
   end

   con:bulk_insert_done()

-- HISTORY TABLE

   con:bulk_insert_init("INSERT INTO history" .. table_num .. [[
	  (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) values]])

   for d_id = 1 , DIST_PER_WARE do
   for c_id = 1 , CUST_PER_DIST do
 
      query = string.format([[(%d, %d, %d, %d, %d, NOW(), 10, '%s' )]],
	c_id, d_id, warehouse_num, d_id, warehouse_num, 
	string.rep(sysbench.rand.string("@"),sysbench.rand.uniform(12,24))
        )
      con:bulk_insert_next(query)

   end 
   end

   con:bulk_insert_done()

    local tab = {}
    local a_counts = {}

    for i = 1, 3000 do
        tab[i] = i
    end

    for i = 1, 3000 do
        local j = math.random(i, 3000)
        tab[i], tab[j] = tab[j], tab[i]
    end

   con:bulk_insert_init("INSERT INTO orders" .. table_num .. [[
	  (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) values]])

   a_counts[warehouse_num] = {}

   for d_id = 1 , DIST_PER_WARE do
   a_counts[warehouse_num][d_id] = {}
   for o_id = 1 , 3000 do
-- 3,000 rows in the ORDER table with
   a_counts[warehouse_num][d_id][o_id] = sysbench.rand.uniform(5,15)
 
      query = string.format([[(%d, %d, %d, %d, NOW(), %s, %d, 1 )]],
	o_id, d_id, warehouse_num, tab[o_id], 
        o_id < 2101 and sysbench.rand.uniform(1,10) or "NULL",
        a_counts[warehouse_num][d_id][o_id]
        )
      con:bulk_insert_next(query)

   end 
   end

   con:bulk_insert_done()

-- STOCK table

   con:bulk_insert_init("INSERT INTO stock" .. table_num .. 
	" (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, "..
        " s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) values")

   for s_id = 1 , 100000 do
 
      query = string.format([[(%d, %d, %d,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',0,0,0,'%s')]],
	s_id, warehouse_num, sysbench.rand.uniform(10,100),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),24),
	string.rep(sysbench.rand.string("@"),sysbench.rand.uniform(26,50)))
      con:bulk_insert_next(query)

   end 
   con:bulk_insert_done()

   con:query(string.format("INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id) SELECT o_id, o_d_id, o_w_id FROM orders%d WHERE o_id>2100 and o_w_id=%d", table_num, table_num, warehouse_num))

   con:bulk_insert_init("INSERT INTO order_line" .. table_num .. [[
	  (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, 
           ol_quantity, ol_amount, ol_dist_info ) values]])

   for d_id = 1 , DIST_PER_WARE do
   for o_id = 1 , 3000 do
   for ol_id = 1, a_counts[warehouse_num][d_id][o_id] do 
 
      query = string.format([[(%d, %d, %d, %d, %d, %d, %s, 5, %f, '%s' )]],
	    o_id, d_id, warehouse_num, ol_id, sysbench.rand.uniform(1, MAXITEMS), warehouse_num,
        o_id < 2101 and "NOW()" or "NULL", 
        o_id < 2101 and 0 or sysbench.rand.uniform_double()*9999.99,
	string.rep(sysbench.rand.string("@"),24)
        )
      res=con:bulk_insert_next(query)
      
   end
   end 
   end

   con:bulk_insert_done()

  end

   if drv:name() == "mysql"
   then
--   	con:query("SET @trx = (SELECT @@global.innodb_flush_log_at_trx_commit=0)")
--   	con:query("SET GLOBAL innodb_flush_log_at_trx_commit=@trx")
   end

end


function thread_done()
   con:disconnect()
end

function cleanup()

   local drv,con = db_connection_init()

   for i = 1, sysbench.opt.tables do
      print(string.format("Dropping tables '%d'...", i))
      con:query("DROP TABLE IF EXISTS history" .. i )
      con:query("DROP TABLE IF EXISTS new_orders" .. i )
      con:query("DROP TABLE IF EXISTS order_line" .. i )
      con:query("DROP TABLE IF EXISTS orders" .. i )
      con:query("DROP TABLE IF EXISTS customer" .. i )
      con:query("DROP TABLE IF EXISTS district" .. i )
      con:query("DROP TABLE IF EXISTS stock" .. i )
      con:query("DROP TABLE IF EXISTS item" .. i )
      con:query("DROP TABLE IF EXISTS warehouse" .. i )
   end
end

function Lastname(num)
  local n = {"BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"}

  name =n[math.floor(num / 100) + 1] .. n[ math.floor(num / 10)%10 + 1] .. n[num%10 + 1]

  return name
end

local init_rand=1
local C_255
local C_1023
local C_8191

function NURand (A, x, y)
	local C

	if init_rand 
	then
		C_255 = sysbench.rand.uniform(0, 255)
		C_1023 = sysbench.rand.uniform(0, 1023)
		C_8191 = sysbench.rand.uniform(0, 8191)
		init_rand = 0
	end

	if A==255
	then
		C = C_255
	elseif A==1023
	then
		C = C_1023
	elseif A==8191
	then
		C = C_8191
	end

	-- return ((( sysbench.rand.uniform(0, A) | sysbench.rand.uniform(x, y)) + C) % (y-x+1)) + x;
	local i = sysbench.rand.uniform(0, A)
	local j = sysbench.rand.uniform(x, y)

	return ((( bit.bor(i, j) ) + C) % (y-x+1)) + x;
end

-- vim:ts=4 ss=4 sw=4 expandtab
