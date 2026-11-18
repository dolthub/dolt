#!/usr/bin/env sysbench

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

-- ----------------------------------------------------------------------
-- TPCC-like workload
-- ----------------------------------------------------------------------

require("tpcc_common")


--
-- produce the id of a valid warehouse other than home_ware
-- (assuming there is one)
--
function other_ware (home_ware)
    local tmp

    if sysbench.opt.scale == 1 then return home_ware end
    repeat
       tmp = sysbench.rand.uniform(1, sysbench.opt.scale)
    until tmp == home_ware
    return tmp
end

function new_order()

-- prep work

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)

    local ol_cnt = sysbench.rand.uniform(5, 15);
    local rbk = sysbench.rand.uniform(1, 100);
    local itemid = {}
    local supware = {}
    local qty = {}
    local all_local = 1

    for i = 1, ol_cnt
    do
        itemid[i] = NURand(8191, 1, MAXITEMS)
        if ((i == ol_cnt - 1) and (rbk == 1))
	then
            itemid[i] = -1
        end
        if sysbench.rand.uniform(1, 100) ~= 1
	then
            supware[i] = w_id
        else 
            supware[i] = other_ware(w_id)
            all_local = 0
        end
        qty[i] = sysbench.rand.uniform(1, 10)
   end


--  SELECT c_discount, c_last, c_credit, w_tax
--  INTO :c_discount, :c_last, :c_credit, :w_tax
--  FROM customer, warehouse
--  WHERE w_id = :w_id 
--  AND c_w_id = w_id 
--  AND c_d_id = :d_id 
--  AND c_id = :c_id;

  if (sysbench.opt.splittable == "yes")
  then
      con:query("START TRANSACTION READ WRITE")
  else
      con:query("BEGIN")
  end

  local c_discount
  local c_last
  local c_credit
  local w_tax

  c_discount, c_last, c_credit, w_tax = con:query_row(([[SELECT c_discount, c_last, c_credit, w_tax 
                                                           FROM customer%d, warehouse%d
                                                          WHERE w_id = %d 
                                                            AND c_w_id = w_id 
                                                            AND c_d_id = %d 
                                                            AND c_id = %d]]):
                                                         format(table_num, table_num, w_id, d_id, c_id))

--        SELECT d_next_o_id, d_tax INTO :d_next_o_id, :d_tax
--                FROM district
--                WHERE d_id = :d_id
--                AND d_w_id = :w_id
--                FOR UPDATE
  local d_next_o_id
  local d_tax

  d_next_o_id, d_tax = con:query_row(([[SELECT d_next_o_id, d_tax 
                                          FROM district%d 
                                         WHERE d_w_id = %d 
                                           AND d_id = %d FOR UPDATE]]):
                                        format(table_num, w_id, d_id))

-- UPDATE district SET d_next_o_id = :d_next_o_id + 1
--                WHERE d_id = :d_id 
--                AND d_w_id = :w_id;

  con:query(([[UPDATE district%d
                  SET d_next_o_id = %d
                WHERE d_id = %d AND d_w_id= %d]]):format(table_num, d_next_o_id + 1, d_id, w_id))

--INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id,
--                                    o_entry_d, o_ol_cnt, o_all_local)
--                VALUES(:o_id, :d_id, :w_id, :c_id, 
--                       :datetime,
--                       :o_ol_cnt, :o_all_local);

  con:query(([[INSERT INTO orders%d
                           (o_id, o_d_id, o_w_id, o_c_id,  o_entry_d, o_ol_cnt, o_all_local)
                    VALUES (%d,%d,%d,%d,NOW(),%d,%d)]]):
                    format(table_num, d_next_o_id, d_id, w_id, c_id, ol_cnt, all_local))

-- INSERT INTO new_orders (no_o_id, no_d_id, no_w_id)
--    VALUES (:o_id,:d_id,:w_id); */

  con:query(([[INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id)
                    VALUES (%d,%d,%d)]]):
                   format(table_num, d_next_o_id, d_id, w_id))

  for ol_number=1, ol_cnt do
	local ol_supply_w_id = supware[ol_number]
	local ol_i_id = itemid[ol_number]
	local ol_quantity = qty[ol_number]

-- SELECT i_price, i_name, i_data
--	INTO :i_price, :i_name, :i_data
--	FROM item
--	WHERE i_id = :ol_i_id;*/

	rs = con:query(([[SELECT i_price, i_name, i_data 
	                    FROM item%d
	                   WHERE i_id = %d]]):
	                  format(table_num, ol_i_id))

	local i_price
	local i_name
	local i_data

	if rs.nrows == 0 then
--          print("ROLLBACK")
          ffi.C.sb_counter_inc(sysbench.tid, ffi.C.SB_CNT_ERROR)
          con:query("ROLLBACK")
	  return	
        end
        
        i_price, i_name, i_data = unpack(rs:fetch_row(), 1, rs.nfields)
        
-- SELECT s_quantity, s_data, s_dist_01, s_dist_02,
--		s_dist_03, s_dist_04, s_dist_05, s_dist_06,
--		s_dist_07, s_dist_08, s_dist_09, s_dist_10
--	INTO :s_quantity, :s_data, :s_dist_01, :s_dist_02,
--	     :s_dist_03, :s_dist_04, :s_dist_05, :s_dist_06,
--	     :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
--	FROM stock
--	WHERE s_i_id = :ol_i_id 
--	AND s_w_id = :ol_supply_w_id
--	FOR UPDATE;*/

        local s_quantity 
        local s_data 
        local ol_dist_info

	s_quantity, s_data, ol_dist_info = con:query_row(([[SELECT s_quantity, s_data, s_dist_%s s_dist 
	                                                      FROM stock%d  
	                                                     WHERE s_i_id = %d AND s_w_id= %d FOR UPDATE]]):
	                                                     format(string.format("%02d",d_id),table_num,ol_i_id,ol_supply_w_id ))
     
        s_quantity=tonumber(s_quantity)
  	if (s_quantity > ol_quantity) then
	        s_quantity = s_quantity - ol_quantity
	else
		s_quantity = s_quantity - ol_quantity + 91
	end

-- UPDATE stock SET s_quantity = :s_quantity
--	WHERE s_i_id = :ol_i_id 
--	AND s_w_id = :ol_supply_w_id;*/

	con:query(([[UPDATE stock%d
	                SET s_quantity = %d
	              WHERE s_i_id = %d 
		        AND s_w_id= %d]]):
		    format(table_num, s_quantity, ol_i_id, ol_supply_w_id))
   
        i_price=tonumber(i_price)
        w_tax=tonumber(w_tax)
        d_tax=tonumber(d_tax)        
        c_discount=tonumber(c_discount)
        
	ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);

-- INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, 
--				 ol_number, ol_i_id, 
--				 ol_supply_w_id, ol_quantity, 
--				 ol_amount, ol_dist_info)
--	VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id,
--		:ol_supply_w_id, :ol_quantity, :ol_amount,
--		:ol_dist_info);

	con:query(([[INSERT INTO order_line%d
                                 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
	                  VALUES (%d,%d,%d,%d,%d,%d,%d,%d,'%s')]]):
	                  format(table_num, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info))

  end

  con:query("COMMIT")

end

function payment()
-- prep work

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local h_amount = sysbench.rand.uniform(1,5000)
    local byname
    local c_w_id
    local c_d_id
    local c_last = Lastname(NURand(255,0,999))

    if sysbench.rand.uniform(1, 100) <= 60 then
        byname = 1 -- select by last name 
    else
        byname = 0 -- select by customer id 
    end

    if sysbench.rand.uniform(1, 100) <= 85 then
        c_w_id = w_id
        c_d_id = d_id
    else
        c_w_id = other_ware(w_id)
        c_d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    end

--  UPDATE warehouse SET w_ytd = w_ytd + :h_amount
--  WHERE w_id =:w_id

  if (sysbench.opt.splittable == "yes")
  then
      con:query("START TRANSACTION READ WRITE")
  else
      con:query("BEGIN")
  end

  con:query(([[UPDATE warehouse%d
	          SET w_ytd = w_ytd + %d 
	        WHERE w_id = %d]]):format(table_num, h_amount, w_id ))

-- SELECT w_street_1, w_street_2, w_city, w_state, w_zip,
--		w_name
--		INTO :w_street_1, :w_street_2, :w_city, :w_state,
--			:w_zip, :w_name
--		FROM warehouse
--		WHERE w_id = :w_id;*/
  local w_street_1, w_street_2, w_city, w_state, w_zip, w_name

  w_street_1, w_street_2, w_city, w_state, w_zip, w_name =
                          con:query_row(([[SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name 
                                             FROM warehouse%d  
                                            WHERE w_id = %d]]):format(table_num, w_id))

-- UPDATE district SET d_ytd = d_ytd + :h_amount
--		WHERE d_w_id = :w_id 
--		AND d_id = :d_id;*/

  con:query(([[UPDATE district%d 
                 SET d_ytd = d_ytd + %d 
               WHERE d_w_id = %d 
                 AND d_id= %d]]):format(table_num, h_amount, w_id, d_id))


  local d_street_1,d_street_2, d_city, d_state, d_zip, d_name

  d_street_1,d_street_2, d_city, d_state, d_zip, d_name = 
                          con:query_row(([[SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name 
                                             FROM district%d
                                            WHERE d_w_id = %d 
                                              AND d_id = %d]]):format(table_num, w_id, d_id ))

  if byname == 1 then

-- SELECT count(c_id) 
--	FROM customer
--	WHERE c_w_id = :c_w_id
--	AND c_d_id = :c_d_id
--	AND c_last = :c_last;*/
  
	local namecnt = con:query_row(([[SELECT count(c_id) namecnt
			                   FROM customer%d
			                  WHERE c_w_id = %d 
			                    AND c_d_id= %d
                                            AND c_last='%s']]):format(table_num, c_w_id, c_d_id, c_last ))
--		SELECT c_id
--		FROM customer
--		WHERE c_w_id = :c_w_id 
--		AND c_d_id = :c_d_id 
--		AND c_last = :c_last
--		ORDER BY c_first;

	if tonumber(namecnt) >1 and namecnt % 2 == 1 then
		namecnt = namecnt + 1
	end

	rs = con:query(([[SELECT c_id
		 	    FROM customer%d
			   WHERE c_w_id = %d AND c_d_id= %d
                             AND c_last='%s' ORDER BY c_first]]
			):format(table_num, c_w_id, c_d_id, c_last ))

	for i = 1,  (namecnt / 2 ) + 1 do
		row = rs:fetch_row()
		c_id = row[1]
	end
  end -- byname

-- SELECT c_first, c_middle, c_last, c_street_1,
--		c_street_2, c_city, c_state, c_zip, c_phone,
--		c_credit, c_credit_lim, c_discount, c_balance,
--		c_since
--	FROM customer
--	WHERE c_w_id = :c_w_id 
--	AND c_d_id = :c_d_id 
--	AND c_id = :c_id
--	FOR UPDATE;

  local c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
        c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since

  c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
  c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since =
	 con:query_row(([[SELECT c_first, c_middle, c_last, c_street_1,
                                 c_street_2, c_city, c_state, c_zip, c_phone,
                                 c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_since
			    FROM customer%d
			   WHERE c_w_id = %d 
			     AND c_d_id= %d
			     AND c_id=%d FOR UPDATE]])
			 :format(table_num, c_w_id, c_d_id, c_id ))

  c_balance = tonumber(c_balance) - h_amount
  c_ytd_payment = tonumber(c_ytd_payment) + h_amount

  if c_credit == "BC" then
-- SELECT c_data 
--	INTO :c_data
--	FROM customer
--	WHERE c_w_id = :c_w_id 
--	AND c_d_id = :c_d_id 
-- 	AND c_id = :c_id; */
    
        local c_data
        c_data = con:query_row(([[SELECT c_data
                                    FROM customer%d
                                   WHERE c_w_id = %d 
                                     AND c_d_id=%d
                                     AND c_id= %d]]):
                                  format(table_num, c_w_id, c_d_id, c_id ))

        local c_new_data=string.sub(string.format("| %4d %2d %4d %2d %4d $%7.2f %12s %24s",
                c_id, c_d_id, c_w_id, d_id, w_id, h_amount, os.time(), c_data), 1, 500);

    --		UPDATE customer
    --			SET c_balance = :c_balance, c_data = :c_new_data
    --			WHERE c_w_id = :c_w_id 
    --			AND c_d_id = :c_d_id 
    --			AND c_id = :c_id
        con:query(([[UPDATE customer%d
                        SET c_balance=%f, c_ytd_payment=%f, c_data='%s'
                      WHERE c_w_id = %d 
                        AND c_d_id=%d
                        AND c_id=%d]])
		  :format(table_num, c_balance, c_ytd_payment, c_new_data, c_w_id, c_d_id, c_id  ))
  else
        con:query(([[UPDATE customer%d
                        SET c_balance=%f, c_ytd_payment=%f
                      WHERE c_w_id = %d 
                        AND c_d_id=%d
                        AND c_id=%d]])
		  :format(table_num, c_balance, c_ytd_payment, c_w_id, c_d_id, c_id  ))

  end

--	INSERT INTO history(h_c_d_id, h_c_w_id, h_c_id, h_d_id,
--			                   h_w_id, h_date, h_amount, h_data)
--	                VALUES(:c_d_id, :c_w_id, :c_id, :d_id,
--		               :w_id, 
--			       :datetime,
--			       :h_amount, :h_data);*/
			       
  con:query(([[INSERT INTO history%d
                           (h_c_d_id, h_c_w_id, h_c_id, h_d_id,  h_w_id, h_date, h_amount, h_data)
                    VALUES (%d,%d,%d,%d,%d,NOW(),%d,'%s')]])
            :format(table_num, c_d_id, c_w_id, c_id, d_id,  w_id, h_amount, string.format("%10s %10s   ",w_name,d_name)))

  con:query("COMMIT")

end

function orderstatus()

    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local byname
    local c_last = Lastname(NURand(255,0,999))

    if sysbench.rand.uniform(1, 100) <= 60 then
        byname = 1 -- select by last name 
    else
        byname = 0 -- select by customer id 
    end

    local c_balance
    local c_first
    local c_middle

    if (sysbench.opt.splittable == "yes")
    then
        con:query("START TRANSACTION READ ONLY")
    else
        con:query("BEGIN")
    end

    if byname == 1 then
--    /*EXEC_SQL SELECT count(c_id)
--            FROM customer
--        WHERE c_w_id = :c_w_id
--        AND c_d_id = :c_d_id
--            AND c_last = :c_last;*/

        local namecnt
        namecnt = con:query_row(([[SELECT count(c_id) namecnt
                                     FROM customer%d
                                    WHERE c_w_id = %d 
                                      AND c_d_id= %d
                                      AND c_last='%s']]):
                                  format(table_num, w_id, d_id, c_last ))

--            SELECT c_balance, c_first, c_middle, c_id
--            FROM customer
--            WHERE c_w_id = :c_w_id
--        AND c_d_id = :c_d_id
--        AND c_last = :c_last
--        ORDER BY c_first;

        rs = con:query(([[SELECT c_balance, c_first, c_middle, c_id
                            FROM customer%d
                	   WHERE c_w_id = %d 
                  	     AND c_d_id= %d
                             AND c_last='%s' ORDER BY c_first]])
		:format(table_num, w_id, d_id, c_last ))

        if tonumber(namecnt) >1 and namecnt % 2 == 1 then
            namecnt = namecnt + 1
        end
        for i = 1,  (namecnt / 2 ) + 1 do
            row = rs:fetch_row()
            c_balance = row[1]
            c_first = row[2]
            c_middle = row[3]
            c_id = row[4]
        end
    else
--		SELECT c_balance, c_first, c_middle, c_last
--		        FROM customer
--		        WHERE c_w_id = :c_w_id
--			AND c_d_id = :c_d_id
--			AND c_id = :c_id;*/
        c_balance, c_first, c_middle, c_last = 
                   con:query_row(([[SELECT c_balance, c_first, c_middle, c_last
                                      FROM customer%d
                   	             WHERE c_w_id = %d 
                   	               AND c_d_id=%d
                                       AND c_id=%d]])
                                  :format(table_num, w_id, d_id, c_id ))
    end
--[=[ Initial query
        SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) FROM orders 
        WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ? AND o_id = 
        (SELECT MAX(o_id) FROM orders WHERE o_w_id = ? AND o_d_id = ? AND o_c_id = ?)

        rs = con:query(([[SELECT o_id, o_entry_d, COALESCE(o_carrier_id,0) 
                  FROM orders%d WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d AND o_id = 
                  (SELECT MAX(o_id) FROM orders%d WHERE o_w_id = %d AND o_d_id = %d AND o_c_id = %d)]])
                  :format(table_num, w_id, d_id, c_id, table_num, w_id, d_id, c_id)) 
--]=]
                  
--[[ Query from tpcc standard

  EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
  INTO :o_id, :o_carrier_id, :entdate
  FROM orders
  ORDER BY o_id DESC;
-]]
      local o_id

      o_id = con:query_row(([[SELECT o_id, o_carrier_id, o_entry_d
                                FROM orders%d 
                               WHERE o_w_id = %d 
                                 AND o_d_id = %d 
                                 AND o_c_id = %d 
                                  ORDER BY o_id DESC]]):
                             format(table_num, w_id, d_id, c_id))

--      rs = con:query(([[SELECT o_id, o_carrier_id, o_entry_d
--                                FROM orders%d 
--                              WHERE o_w_id = %d 
--                                 AND o_d_id = %d 
--                                 AND o_c_id = %d 
--                                  ORDER BY o_id DESC]]):
--                             format(table_num, w_id, d_id, c_id))
--     if rs.nrows == 0 then
--	print(string.format("Error o_id %d, %d, %d, %d\n", table_num , w_id , d_id , c_id))
--     end
--    for i = 1,  rs.nrows do
--        row = rs:fetch_row()
--	o_id= row[1] 
--    end

--		SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount,
--                       ol_delivery_d
--		FROM order_line
--	        WHERE ol_w_id = :c_w_id
--		AND ol_d_id = :c_d_id
--		AND ol_o_id = :o_id;*/

    rs = con:query(([[SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
            FROM order_line%d WHERE ol_w_id = %d AND ol_d_id = %d  AND ol_o_id = %d]])
                  :format(table_num, w_id, d_id, d_id, o_id))
    for i = 1,  rs.nrows do
        row = rs:fetch_row()
        local ol_i_id = row[1]
        local ol_supply_w_id = row[2]
        local ol_quantity = row[3]
        local ol_amount = row[4]
        local ol_delivery_d = row[5]
    end
    con:query("COMMIT")

end

function delivery()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local o_carrier_id = sysbench.rand.uniform(1, 10)

    if (sysbench.opt.splittable == "yes")
    then
        con:query("START TRANSACTION READ WRITE")
    else
        con:query("BEGIN")
    end

    for  d_id = 1, DIST_PER_WARE do

--	SELECT COALESCE(MIN(no_o_id),0) INTO :no_o_id
--		                FROM new_orders
--		                WHERE no_d_id = :d_id AND no_w_id = :w_id;*/
		                
--        rs = con:query(([[SELECT COALESCE(MIN(no_o_id),0) no_o_id
--                 FROM new_orders%d WHERE no_d_id = %d AND no_w_id = %d FOR UPDATE]])
--                      :format(table_num, d_id, w_id))

        local no_o_id
        
        rs = con:query(([[SELECT no_o_id
                                     FROM new_orders%d 
                                    WHERE no_d_id = %d 
                                      AND no_w_id = %d 
                                      ORDER BY no_o_id ASC LIMIT 1 FOR UPDATE]])
                                   :format(table_num, d_id, w_id))

        if (rs.nrows > 0) then
          no_o_id=unpack(rs:fetch_row(), 1, rs.nfields)
        end

        if (no_o_id ~= nil ) then 
        
--		DELETE FROM new_orders WHERE no_o_id = :no_o_id AND no_d_id = :d_id
--		  AND no_w_id = :w_id;*/

        con:query(([[DELETE FROM new_orders%d
                           WHERE no_o_id = %d 
                             AND no_d_id = %d  
                             AND no_w_id = %d]])
                            :format(table_num, no_o_id, d_id, w_id))

--  SELECT o_c_id INTO :c_id FROM orders
--		                WHERE o_id = :no_o_id AND o_d_id = :d_id
--				AND o_w_id = :w_id;*/

        local o_c_id
        o_c_id = con:query_row(([[SELECT o_c_id
                                    FROM orders%d 
                                   WHERE o_id = %d 
                                     AND o_d_id = %d 
                                     AND o_w_id = %d]])
                                  :format(table_num, no_o_id, d_id, w_id))

--	 UPDATE orders SET o_carrier_id = :o_carrier_id
--		                WHERE o_id = :no_o_id AND o_d_id = :d_id AND
--				o_w_id = :w_id;*/

        con:query(([[UPDATE orders%d 
                        SET o_carrier_id = %d
                      WHERE o_id = %d 
                        AND o_d_id = %d 
                        AND o_w_id = %d]])
                      :format(table_num, o_carrier_id, no_o_id, d_id, w_id))

--   UPDATE order_line
--		                SET ol_delivery_d = :datetime
--		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id AND
--				ol_w_id = :w_id;*/
        con:query(([[UPDATE order_line%d 
                        SET ol_delivery_d = NOW()
                      WHERE ol_o_id = %d 
                        AND ol_d_id = %d 
                        AND ol_w_id = %d]])
                      :format(table_num, no_o_id, d_id, w_id))

--	 SELECT SUM(ol_amount) INTO :ol_total
--		                FROM order_line
--		                WHERE ol_o_id = :no_o_id AND ol_d_id = :d_id
--				AND ol_w_id = :w_id;*/

        local sm_ol_amount
        sm_ol_amount = con:query_row(([[SELECT SUM(ol_amount) sm
                                          FROM order_line%d 
                                         WHERE ol_o_id = %d 
                                           AND ol_d_id = %d 
                                           AND ol_w_id = %d]])
                                      :format(table_num, no_o_id, d_id, w_id))

--	UPDATE customer SET c_balance = c_balance + :ol_total ,
--		                             c_delivery_cnt = c_delivery_cnt + 1
--		                WHERE c_id = :c_id AND c_d_id = :d_id AND
--				c_w_id = :w_id;*/
--        print(string.format("update customer table %d, cid %d, did %d, wid %d balance %f",table_num, o_c_id, d_id, w_id, sm_ol_amount))  				
        con:query(([[UPDATE customer%d 
                        SET c_balance = c_balance + %f,
                            c_delivery_cnt = c_delivery_cnt + 1
                      WHERE c_id = %d 
                        AND c_d_id = %d 
                        AND c_w_id = %d]])
                      :format(table_num, sm_ol_amount, o_c_id, d_id, w_id))
        end
        
    end
    con:query("COMMIT")

end

function stocklevel()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local level = sysbench.rand.uniform(10, 20)

    if (sysbench.opt.splittable == "yes")
    then
        con:query("START TRANSACTION READ ONLY")
    else
        con:query("BEGIN")
    end

--	/*EXEC_SQL SELECT d_next_o_id
--	                FROM district
--	                WHERE d_id = :d_id
--			AND d_w_id = :w_id;*/

--  What variant of queries to use for stock_level transaction
--  case1 - specification
--  case2 - modified/simplified

    local stock_level_queries="case1" 
    local d_next_o_id
    

    d_next_o_id = con:query_row(([[SELECT d_next_o_id 
                                     FROM district%d
             	                    WHERE d_id = %d AND d_w_id= %d]])
		                  :format( table_num, d_id, w_id))

    if stock_level_queries == "case1" then 

--[[
     SELECT COUNT(DISTINCT (s_i_id)) INTO :stock_count
     FROM order_line, stock
     WHERE ol_w_id=:w_id AND ol_d_id=:d_id AND ol_o_id<:o_id AND  ol_o_id>=:o_id-20 AND s_w_id=:w_id AND s_i_id=ol_i_id AND s_quantity < :threshold;
--]]

    rs = con:query(([[SELECT COUNT(DISTINCT (s_i_id))
                        FROM order_line%d, stock%d
                       WHERE ol_w_id = %d 
                         AND ol_d_id = %d
                         AND ol_o_id < %d 
                         AND ol_o_id >= %d
                         AND s_w_id= %d
                         AND s_i_id=ol_i_id 
                         AND s_quantity < %d ]])
		:format(table_num, table_num, w_id, d_id, d_next_o_id, d_next_o_id - 20, w_id, level ))



--	                SELECT DISTINCT ol_i_id
--	                FROM order_line
--	                WHERE ol_w_id = :w_id
--			AND ol_d_id = :d_id
--			AND ol_o_id < :d_next_o_id
--			AND ol_o_id >= (:d_next_o_id - 20);


    else
    rs = con:query(([[SELECT DISTINCT ol_i_id FROM order_line%d
               WHERE ol_w_id = %d AND ol_d_id = %d
                 AND ol_o_id < %d AND ol_o_id >= %d]])
                :format(table_num, w_id, d_id, d_next_o_id, d_next_o_id - 20 ))

    local ol_i_id = {}

    for i = 1, rs.nrows do
        ol_i_id[i] = unpack(rs:fetch_row(), 1, rs.nfields)
    end

    for i = 1, #ol_i_id do

--       SELECT count(*) INTO :i_count
--                      FROM stock
--                      WHERE s_w_id = :w_id
--                      AND s_i_id = :ol_i_id
--                      AND s_quantity < :level;*/

        rs = con:query(([[SELECT count(*) FROM stock%d
                   WHERE s_w_id = %d AND s_i_id = %d
                   AND s_quantity < %d]])
                :format(table_num, w_id, ol_i_id[i], level ) )
        local cnt
        for i = 1, rs.nrows do
            cnt = unpack(rs:fetch_row(), 1, rs.nfields)
        end

    end
    end

    con:query("COMMIT")

end

-- function purge to remove all orders, this is useful if we want to limit data directory in size

function purge()
    for i = 1, 10 do
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)

    if (sysbench.opt.splittable == "yes")
    then
        con:query("START TRANSACTION READ WRITE")
    else
        con:query("BEGIN")
    end

        local m_o_id
        
        rs = con:query(([[SELECT min(no_o_id) mo
                                     FROM new_orders%d 
                                    WHERE no_w_id = %d AND no_d_id = %d]])
                                   :format(table_num, w_id, d_id))

        if (rs.nrows > 0) then
          m_o_id=unpack(rs:fetch_row(), 1, rs.nfields)
        end

        if (m_o_id ~= nil ) then 
-- select o_id,o.o_d_id from orders2 o, (select o_c_id,o_w_id,o_d_id,count(distinct o_id) from orders2 where o_w_id=1  and o_id > 2100 and o_id < 11153 group by o_c_id,o_d_id,o_w_id having count( distinct o_id) > 1 limit 1) t where t.o_w_id=o.o_w_id and t.o_d_id=o.o_d_id and t.o_c_id=o.o_c_id limit 1;
	-- find an order to delete
        rs = con:query(([[SELECT o_id FROM orders%d o, (SELECT o_c_id,o_w_id,o_d_id,count(distinct o_id) FROM orders%d WHERE o_w_id=%d AND o_d_id=%d AND o_id > 2100 AND o_id < %d GROUP BY o_c_id,o_d_id,o_w_id having count( distinct o_id) > 1 limit 1) t WHERE t.o_w_id=o.o_w_id and t.o_d_id=o.o_d_id and t.o_c_id=o.o_c_id limit 1 ]])
                                   :format(table_num, table_num, w_id, d_id, m_o_id))
	
        local del_o_id
        if (rs.nrows > 0) then
          del_o_id=unpack(rs:fetch_row(), 1, rs.nfields)
        end

        if (del_o_id ~= nil ) then 
        
        con:query(([[DELETE FROM order_line%d where ol_w_id=%d AND ol_d_id=%d AND ol_o_id=%d]])
                            :format(table_num, w_id, d_id, del_o_id))
        con:query(([[DELETE FROM orders%d where o_w_id=%d AND o_d_id=%d and o_id=%d]])
                            :format(table_num, w_id, d_id, del_o_id))

        if drv:name() == "mysql" then   --Use MySQL specific SQL which allows LIMIT clause for DELETE statement
          con:query(([[DELETE FROM history%d where h_w_id=%d AND h_d_id=%d LIMIT 10]])
                              :format(table_num, w_id, d_id ))
        elseif drv:name() == "pgsql" then --Use ctid to delete
          con:query(([[DELETE FROM history%d where ctid IN (SELECT ctid FROM history%d WHERE h_w_id=%d AND h_d_id=%d LIMIT 10)]])
                              :format(table_num,table_num, w_id, d_id ))
        end


	end

        end
        
    con:query("COMMIT")
    end
end

-- ----------------------------------------------------------------------
-- Isolated new_order statements
--
-- Each new_order_N runs exactly one of the statements from new_order() in
-- its own transaction so it can be benchmarked on its own, e.g.
--   trx="new_order_3"
-- Values that new_order() would read from an earlier statement are generated
-- instead, except for the INSERTs: those run with the statements that produce
-- their values in new_order().
--
--   new_order_1  SELECT customer/warehouse
--   new_order_2  SELECT district ... FOR UPDATE
--   new_order_3  UPDATE district (d_next_o_id)
--   new_order_4  SELECT + UPDATE district, INSERT orders
--   new_order_5  SELECT + UPDATE district, INSERT new_orders
--   new_order_6  SELECT item
--   new_order_7  SELECT stock ... FOR UPDATE
--   new_order_8  UPDATE stock (s_quantity)
--   new_order_9  SELECT customer/warehouse, SELECT + UPDATE district,
--                SELECT item, SELECT stock, INSERT order_line
--
-- Every transaction commits. The INSERTs take their o_id from d_next_o_id and
-- increment it, as new_order() does, so committed keys never collide.
-- ----------------------------------------------------------------------

local function select_district(table_num, w_id, d_id)
    return con:query_row(([[SELECT d_next_o_id, d_tax
                              FROM district%d
                             WHERE d_w_id = %d
                               AND d_id = %d FOR UPDATE]]):
                            format(table_num, w_id, d_id))
end

local function next_order_id(table_num, w_id, d_id)
    local d_next_o_id, d_tax = select_district(table_num, w_id, d_id)
    con:query(([[UPDATE district%d
                    SET d_next_o_id = %d
                  WHERE d_id = %d AND d_w_id= %d]]):format(table_num, d_next_o_id + 1, d_id, w_id))
    return d_next_o_id, d_tax
end


function new_order_1()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)

    con:query("BEGIN")
    con:query_row(([[SELECT c_discount, c_last, c_credit, w_tax
                       FROM customer%d, warehouse%d
                      WHERE w_id = %d
                        AND c_w_id = w_id
                        AND c_d_id = %d
                        AND c_id = %d]]):
                     format(table_num, table_num, w_id, d_id, c_id))
    con:query("COMMIT")
end

function new_order_2()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)

    con:query("BEGIN")
    con:query_row(([[SELECT d_next_o_id, d_tax
                       FROM district%d
                      WHERE d_w_id = %d
                        AND d_id = %d FOR UPDATE]]):
                     format(table_num, w_id, d_id))
    con:query("COMMIT")
end

function new_order_3()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)

    -- new_order() sets a literal it read in the previous statement; increment
    -- in place instead so d_next_o_id stays valid for later new_order() runs
    con:query("BEGIN")
    con:query(([[UPDATE district%d
                    SET d_next_o_id = d_next_o_id + 1
                  WHERE d_id = %d AND d_w_id= %d]]):format(table_num, d_id, w_id))
    con:query("COMMIT")
end

function new_order_4()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local ol_cnt = sysbench.rand.uniform(5, 15)
    local all_local = sysbench.rand.uniform(1, 100) ~= 1 and 1 or 0

    con:query("BEGIN")
    local d_next_o_id = next_order_id(table_num, w_id, d_id)
    con:query(([[INSERT INTO orders%d
                    (o_id, o_d_id, o_w_id, o_c_id,  o_entry_d, o_ol_cnt, o_all_local)
                VALUES (%d,%d,%d,%d,NOW(),%d,%d)]]):
                format(table_num, o_id, d_id, w_id, c_id, ol_cnt, all_local))
    con:query("COMMIT")
end

function new_order_5()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)

    con:query("BEGIN")
    local d_next_o_id = next_order_id(table_num, w_id, d_id)
    con:query(([[INSERT INTO new_orders%d (no_o_id, no_d_id, no_w_id)
                      VALUES (%d,%d,%d)]]):
                     format(table_num, d_next_o_id, d_id, w_id))
    con:query("COMMIT")
end

function new_order_6()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local ol_i_id = NURand(8191, 1, MAXITEMS)

    con:query("BEGIN")
    con:query(([[SELECT i_price, i_name, i_data
                   FROM item%d
                  WHERE i_id = %d]]):
                 format(table_num, ol_i_id))
    con:query("COMMIT")
end

function new_order_7()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local ol_i_id = NURand(8191, 1, MAXITEMS)

    con:query("BEGIN")
    con:query_row(([[SELECT s_quantity, s_data, s_dist_%s s_dist
                       FROM stock%d
                      WHERE s_i_id = %d AND s_w_id= %d FOR UPDATE]]):
                     format(string.format("%02d", d_id), table_num, ol_i_id, w_id))
    con:query("COMMIT")
end

function new_order_8()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local ol_i_id = NURand(8191, 1, MAXITEMS)
    -- same range the loader uses for s_quantity
    local s_quantity = sysbench.rand.uniform(10, 100)

    con:query("BEGIN")
    con:query(([[UPDATE stock%d
                    SET s_quantity = %d
                  WHERE s_i_id = %d
                    AND s_w_id= %d]]):
                 format(table_num, s_quantity, ol_i_id, w_id))
    con:query("COMMIT")
end

function new_order_9()
    local table_num = sysbench.rand.uniform(1, sysbench.opt.tables)
    local w_id = sysbench.rand.uniform(1, sysbench.opt.scale)
    local d_id = sysbench.rand.uniform(1, DIST_PER_WARE)
    local c_id = NURand(1023, 1, CUST_PER_DIST)
    local ol_number = 1
    local ol_i_id = NURand(8191, 1, MAXITEMS)
    local ol_supply_w_id = w_id
    if sysbench.rand.uniform(1, 100) == 1 then
        ol_supply_w_id = other_ware(w_id)
    end
    local ol_quantity = sysbench.rand.uniform(1, 10)

    con:query("BEGIN")

    local c_discount, c_last, c_credit, w_tax = con:query_row(([[SELECT c_discount, c_last, c_credit, w_tax
                                                                   FROM customer%d, warehouse%d
                                                                  WHERE w_id = %d
                                                                    AND c_w_id = w_id
                                                                    AND c_d_id = %d
                                                                    AND c_id = %d]]):
                                                                 format(table_num, table_num, w_id, d_id, c_id))

    local d_next_o_id, d_tax = next_order_id(table_num, w_id, d_id)

    local i_price = con:query_row(([[SELECT i_price, i_name, i_data
                                       FROM item%d
                                      WHERE i_id = %d]]):
                                     format(table_num, ol_i_id))

    local s_quantity, s_data, ol_dist_info = con:query_row(([[SELECT s_quantity, s_data, s_dist_%s s_dist
                                                                FROM stock%d
                                                               WHERE s_i_id = %d AND s_w_id= %d FOR UPDATE]]):
                                                               format(string.format("%02d", d_id), table_num, ol_i_id, ol_supply_w_id))

    local ol_amount = ol_quantity * tonumber(i_price) * (1 + tonumber(w_tax) + tonumber(d_tax)) * (1 - tonumber(c_discount))

    con:query(([[INSERT INTO order_line%d
                             (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
                      VALUES (%d,%d,%d,%d,%d,%d,%d,%d,'%s')]]):
                      format(table_num, d_next_o_id, d_id, w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info))
    con:query("COMMIT")
end

-- vim:ts=4 ss=4 sw=4 expandtab
