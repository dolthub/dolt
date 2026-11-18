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
-- Check data code for TPCC benchmarks.
-- -----------------------------------------------------------------------------


require("tpcc_common")


function check_tables(drv, con, warehouse_num)

    straight_join_hint=","
    
    if drv:name() == "mysql" then
      straight_join_hint = "STRAIGHT_JOIN"
    end   


    local pass1 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query("SELECT d_w_id,sum(d_ytd)-max(w_ytd) diff FROM district"..table_num..",warehouse"..table_num.." WHERE d_w_id=w_id AND w_id="..warehouse_num.." group by d_w_id") 
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d_tax = tonumber(row[2])
            if d_tax ~= 0 then
                pass1=0
                print(string.format("Check 1, warehouse: %d, table %d FAILED!!!", warehouse_num, table_num))
            end
        end
    end
    
    if pass1 ~= 1 then
        print(string.format("Check 1, warehouse: %d FAILED!!!", warehouse_num))
    else
        print(string.format("Check 1, warehouse: %d PASSED", warehouse_num))
    end

-- CHECK 2 
-- select dis.d_id, d_next_o_id-1,mo,mno from district1 dis, (select o_d_id,max(o_id) mo from orders1 where o_w_id=1 group by o_d_id) q, (select no_d_id,max(no_o_id) mno from new_orders1 where no_w_id=1 group by no_d_id) no where d_w_id=1 and q.o_d_id=dis.d_id and no.no_d_id=dis.d_id


    local pass2 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format("SELECT dis.d_id, d_next_o_id-1,mo,mno FROM district%d dis, (SELECT o_d_id,max(o_id) mo FROM orders%d WHERE o_w_id=%d GROUP BY o_d_id) q, (select no_d_id,max(no_o_id) mno from new_orders%d where no_w_id=%d group by no_d_id) no where d_w_id=%d and q.o_d_id=dis.d_id and no.no_d_id=dis.d_id", table_num,table_num,warehouse_num, table_num, warehouse_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[2])
            local d2 = tonumber(row[3])
            local d3 = tonumber(row[4])
            if d1 ~= d2 then
                pass2=0
            end
            if d1 ~= d3 then
                pass2=0
            end
        end
    end
    
    if pass2 == 1 then
        print(string.format("Check 2, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 2, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass3 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format("select no_d_id,max(no_o_id)-min(no_o_id)+1,count(*) from new_orders%d where no_w_id=%d group by no_d_id",table_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[2])
            local d2 = tonumber(row[3])
            if d1 ~= d2 then
                pass3=0
            end
        end
    end
    
    if pass3 == 1 then
        print(string.format("Check 3, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 3, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass4 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format([[SELECT count(*) 
                                         FROM (SELECT o_d_id, SUM(o_ol_cnt) sm1, MAX(cn) as cn
                                                 FROM orders%d,(SELECT ol_d_id, COUNT(*) cn 
                                                                  FROM order_line%d 
                                                                 WHERE ol_w_id=%d GROUP BY ol_d_id) ol 
                                                WHERE o_w_id=%d AND ol_d_id=o_d_id GROUP BY o_d_id) t1 
                                         WHERE sm1<>cn]],table_num, table_num, warehouse_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[1])
            if d1 ~= 0 then
                pass4=0
            end
        end
    end
    
    if pass4 == 1 then
        print(string.format("Check 4, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 4, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass5 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format("SELECT count(*) FROM orders%d LEFT JOIN new_orders%d ON (no_w_id=o_w_id AND o_d_id=no_d_id AND o_id=no_o_id) where o_w_id=%d and ((o_carrier_id IS NULL and no_o_id IS  NULL) OR (o_carrier_id IS NOT NULL and no_o_id IS NOT NULL  )) ",table_num, table_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[1])
            if d1 ~= 0 then
                pass5=0
            end
        end
    end
    
    if pass5 == 1 then
        print(string.format("Check 5, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 5, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass7 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format("SELECT count(*) FROM orders%d, order_line%d WHERE o_id=ol_o_id AND o_d_id=ol_d_id AND ol_w_id=o_w_id AND o_w_id=%d AND ((ol_delivery_d IS NULL and o_carrier_id IS NOT NULL) or (o_carrier_id IS NULL and ol_delivery_d IS NOT NULL ))",table_num, table_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[1])
            if d1 ~= 0 then
                pass7=0
            end
        end
    end
    
    if pass7 == 1 then
        print(string.format("Check 7, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 7, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass8 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format("SELECT count(*) cn FROM (SELECT w_id,w_ytd,SUM(h_amount) sm FROM history%d,warehouse%d WHERE h_w_id=w_id and w_id=%d GROUP BY w_id) t1 WHERE w_ytd<>sm",table_num, table_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[1])
            if d1 ~= 0 then
                pass8=0
            end
        end
    end
    
    if pass8 == 1 then
        print(string.format("Check 8, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 8, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass9 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format("SELECT COUNT(*) FROM (select d_id,d_w_id,sum(d_ytd) s1 from district%d group by d_id,d_w_id) d,(select h_d_id,h_w_id,sum(h_amount) s2 from history%d WHERE  h_w_id=%d group by h_d_id, h_w_id) h WHERE h_d_id=d_id AND d_w_id=h_w_id and d_w_id=%d and s1<>s2",table_num, table_num, warehouse_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[1])
            if d1 ~= 0 then
                pass9=0
            end
        end
    end
    
    if pass9 == 1 then
        print(string.format("Check 9, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 9, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass10 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format([[SELECT count(*) 
                                          FROM (  SELECT  c.c_id, c.c_d_id, c.c_w_id, c.c_balance c1, 
                                                         (SELECT sum(ol_amount) FROM orders%d ]] .. straight_join_hint .. [[ order_line%d 
                                                           WHERE OL_W_ID=O_W_ID 
                                                             AND OL_D_ID = O_D_ID 
                                                             AND OL_O_ID = O_ID 
                                                             AND OL_DELIVERY_D IS NOT NULL 
                                                             AND O_W_ID=c.c_w_id 
                                                             AND O_D_ID=c.C_D_ID 
                                                             AND O_C_ID=c.C_ID) sm, (SELECT  sum(h_amount)  from  history%d 
                                                                                      WHERE H_C_W_ID=c.C_W_ID 
                                                                                        AND H_C_D_ID=c.C_D_ID 
                                                                                        AND H_C_ID=c.C_ID) smh 
                                                   FROM customer%d c 
                                                  WHERE  c.c_w_id=%d ) t 
                                         WHERE c1<>sm-smh]],table_num, table_num, table_num, table_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[1])
            if d1 ~= 0 then
                pass10=0
            end
        end
    end
    
    if pass10 == 1 then
        print(string.format("Check 10, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 10, warehouse: %d FAILED!!!", warehouse_num))
    end

    local pass12 = 1
    for table_num = 1, sysbench.opt.tables do 
        -- print(string.format("Checking  tables: %d for warehouse: %d\n", table_num, warehouse_num))
        rs  = con:query(string.format([[SELECT count(*) FROM (SELECT  c.c_id, c.c_d_id, c.c_balance c1, c_ytd_payment, 
                                         (SELECT sum(ol_amount) FROM orders%d ]] .. straight_join_hint .. [[ order_line%d 
                                         WHERE OL_W_ID=O_W_ID AND OL_D_ID = O_D_ID AND OL_O_ID = O_ID AND OL_DELIVERY_D IS NOT NULL AND 
                                         O_W_ID=c.c_w_id AND O_D_ID=c.C_D_ID AND O_C_ID=c.C_ID) sm FROM customer%d c WHERE  c.c_w_id=%d) t1 
                                         WHERE c1+c_ytd_payment <> sm ]] ,table_num, table_num, table_num, warehouse_num))
        
        for i = 1, rs.nrows do
            row = rs:fetch_row()
            local d1 = tonumber(row[1])
            if d1 ~= 0 then
                pass12=0
            end
        end
    end
    
    if pass12 == 1 then
        print(string.format("Check 12, warehouse: %d PASSED", warehouse_num))
    else
        print(string.format("Check 12, warehouse: %d FAILED!!!", warehouse_num))
    end
end


-- vim:ts=4 ss=4 sw=4 expandtab
