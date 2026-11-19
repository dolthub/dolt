// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package enginetest

import (
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestSysbenchTransactionCV(t *testing.T) {
	harness := newDoltHarness(t)
	defer harness.Close()
	enginetest.TestTransactionScript(t, harness, queries.TransactionTest{
		Name: "Sysbench Transactions Shouldn't Cause Constraint Violations",
		SetUpScript: []string{
			`SET FOREIGN_KEY_CHECKS=0;`,
			`CREATE TABLE IF NOT EXISTS warehouse1 ( w_id smallint not null, w_name varchar(10), w_street_1 varchar(20),
w_street_2 varchar(20), w_city varchar(20), w_state char(2), w_zip char(9), w_tax decimal(4,2), w_ytd decimal(12,2),
primary key (w_id) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS district1 ( d_id tinyint not null, d_w_id smallint not null, d_name varchar(10),
d_street_1 varchar(20), d_street_2 varchar(20), d_city varchar(20), d_state char(2), d_zip char(9), d_tax decimal(4,2),
d_ytd decimal(12,2), d_next_o_id int, primary key (d_w_id, d_id) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS customer1 ( c_id int not null, c_d_id tinyint not null, c_w_id smallint not null,
c_first varchar(16), c_middle char(2), c_last varchar(16), c_street_1 varchar(20), c_street_2 varchar(20),
c_city varchar(20), c_state char(2), c_zip char(9), c_phone char(16), c_since datetime, c_credit char(2),
c_credit_lim bigint, c_discount decimal(4,2), c_balance decimal(12,2), c_ytd_payment decimal(12,2),
c_payment_cnt smallint, c_delivery_cnt smallint, c_data text, PRIMARY KEY(c_w_id, c_d_id, c_id) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS history1 ( h_c_id int, h_c_d_id tinyint, h_c_w_id smallint, h_d_id tinyint,
h_w_id smallint, h_date datetime, h_amount decimal(6,2), h_data varchar(24) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS orders1 ( o_id int not null, o_d_id tinyint not null, o_w_id smallint not null,
o_c_id int, o_entry_d datetime, o_carrier_id tinyint, o_ol_cnt tinyint, o_all_local tinyint,
PRIMARY KEY(o_w_id, o_d_id, o_id) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS new_orders1 ( no_o_id int not null, no_d_id tinyint not null, no_w_id smallint not null,
PRIMARY KEY(no_w_id, no_d_id, no_o_id) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS order_line1 ( ol_o_id int not null, ol_d_id tinyint not null, ol_w_id smallint not null,
ol_number tinyint not null, ol_i_id int, ol_supply_w_id smallint, ol_delivery_d datetime, ol_quantity tinyint,
ol_amount decimal(6,2), ol_dist_info char(24), PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS stock1 ( s_i_id int not null, s_w_id smallint not null, s_quantity smallint,
s_dist_01 char(24), s_dist_02 char(24), s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24),
s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd decimal(8,0), s_order_cnt smallint,
s_remote_cnt smallint, s_data varchar(50), PRIMARY KEY(s_w_id, s_i_id) ) /*! ENGINE = innodb */;`,
			`CREATE TABLE IF NOT EXISTS item1 ( i_id int not null, i_im_id int, i_name varchar(24), i_price decimal(5,2),
i_data varchar(50), PRIMARY KEY(i_id) ) /*! ENGINE = innodb */;`,
			`CREATE INDEX idx_customer1 ON customer1 (c_w_id,c_d_id,c_last,c_first);`,
			`CREATE INDEX idx_orders1 ON orders1 (o_w_id,o_d_id,o_c_id,o_id);`,
			`CREATE INDEX fkey_stock_21 ON stock1 (s_i_id);`,
			`CREATE INDEX fkey_order_line_21 ON order_line1 (ol_supply_w_id,ol_i_id);`,
			`CREATE INDEX fkey_history_11 ON history1 (h_c_w_id,h_c_d_id,h_c_id);`,
			`CREATE INDEX fkey_history_21 ON history1 (h_w_id,h_d_id );`,
			`ALTER TABLE new_orders1 ADD CONSTRAINT fkey_new_orders_1_1 FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES orders1(o_w_id,o_d_id,o_id);`,
			`ALTER TABLE orders1 ADD CONSTRAINT fkey_orders_1_1 FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer1(c_w_id,c_d_id,c_id);`,
			`ALTER TABLE customer1 ADD CONSTRAINT fkey_customer_1_1 FOREIGN KEY(c_w_id,c_d_id) REFERENCES district1(d_w_id,d_id);`,
			`ALTER TABLE history1 ADD CONSTRAINT fkey_history_1_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer1(c_w_id,c_d_id,c_id);`,
			`ALTER TABLE history1 ADD CONSTRAINT fkey_history_2_1 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district1(d_w_id,d_id);`,
			`ALTER TABLE district1 ADD CONSTRAINT fkey_district_1_1 FOREIGN KEY(d_w_id) REFERENCES warehouse1(w_id);`,
			`ALTER TABLE order_line1 ADD CONSTRAINT fkey_order_line_1_1 FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES orders1(o_w_id,o_d_id,o_id);`,
			`ALTER TABLE order_line1 ADD CONSTRAINT fkey_order_line_2_1 FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock1(s_w_id,s_i_id);`,
			`ALTER TABLE stock1 ADD CONSTRAINT fkey_stock_1_1 FOREIGN KEY(s_w_id) REFERENCES warehouse1(w_id);`,
			`ALTER TABLE stock1 ADD CONSTRAINT fkey_stock_2_1 FOREIGN KEY(s_i_id) REFERENCES item1(i_id);`,
			`INSERT INTO warehouse1 (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) values
(1, 'name-ussgn','street1-suwfdxnitk','street2-sdptwkrcjd', 'city-wowgpzhpmq', 'fu', 'zip-12460', 0.116534,300000);`,
			`INSERT INTO district1 (d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) values
(7, 1, 'name-pemzz','street1-odwzoulqwp','street2-fcrhtkuwrm', 'city-gtqousarvt', 'vn', 'zip-70904', 0.131138,30000,3001),
(10, 1, 'name-tpdiw','street1-sqmgopsaft','street2-vulvkewlup', 'city-rdoywkmyxu', 'an', 'zip-80251', 0.057398,30000,3001);`,
			`INSERT INTO customer1 (c_id, c_d_id, c_w_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state,
c_zip, c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) values
(2786, 1, 1, 'first-gwmezwk','OE','OUGHTOUGHTPRES','street1-erszzgjrvj', 'street2-cjgkgzryoy', 'city-ikbbhzmzpu', 'rx',
'zip-55308','6915194643940288',NOW(),'GC',50000,0.158403,-10,10,1,0,'l494' );`,
			`INSERT INTO stock1 (s_i_id, s_w_id, s_quantity, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06,
s_dist_07, s_dist_08, s_dist_09, s_dist_10, s_ytd, s_order_cnt, s_remote_cnt, s_data) values
(3115, 1, 93,'y24','j24','p24','x24','m24','g24','b24','p24','v24','s24',0,0,0,'x28'),
(26507, 1, 49,'e24','m24','i24','y24','h24','f24','u24','x24','m24','d24',0,0,0,'x27'),
(29168, 1, 42,'c24','g24','z24','c24','v24','s24','t24','g24','g24','e24',0,0,0,'o40'),
(38702, 1, 63,'z24','x24','y24','l24','g24','l24','s24','q24','w24','u24',0,0,0,'l35'),
(39823, 1, 39,'v24','r24','h24','n24','v24','u24','r24','r24','t24','c24',0,0,0,'f35'),
(42365, 1, 77,'c24','q24','u24','f24','l24','k24','n24','q24','k24','a24',0,0,0,'f28'),
(53534, 1, 39,'l24','n24','v24','v24','c24','s24','q24','s24','l24','p24',0,0,0,'u24uuuuu'),
(60995, 1, 35,'k24','k24','f24','e24','e24','d24','p24','r24','m24','e24',0,0,0,'s38'),
(62631, 1, 88,'y24','z24','l24','e24','b24','v24','j24','q24','g24','u24',0,0,0,'e39'),
(70752, 1, 80,'k24','o24','u24','p24','c24','d24','t24','d24','h24','r24',0,0,0,'h44'),
(84125, 1, 16,'c24','r24','m24','e24','t24','g24','e24','j24','n24','d24',0,0,0,'y41'),
(87468, 1, 94,'b24','p24','o24','o24','t24','s24','d24','g24','a24','o24',0,0,0,'s24s24s'),
(88405, 1, 60,'c24','o24','u24','k24','n24','d24','u24','q24','t24','s24',0,0,0,'n46');`,
			`SET FOREIGN_KEY_CHECKS=1;`,
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "/* client a */ SET SESSION transaction_isolation='REPEATABLE-READ';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ SET SESSION transaction_isolation='REPEATABLE-READ';",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ SET FOREIGN_KEY_CHECKS=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ SET autocommit=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client b */ SET autocommit=0;",
				Expected: []sql.Row{{types.NewOkResult(0)}},
			},
			{
				Query:    "/* client a */ BEGIN;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ BEGIN;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT * FROM dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ SELECT * FROM dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ UPDATE warehouse1 SET w_ytd = w_ytd + 622 WHERE w_id = 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client a */ UPDATE district1 SET d_ytd = d_ytd + 622 WHERE d_w_id = 1 AND d_id= 10;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client a */ UPDATE customer1 SET c_balance=-632.000000, c_ytd_payment=632.000000 WHERE c_w_id = 1 AND c_d_id=1 AND c_id=2786;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ UPDATE district1 SET d_next_o_id = 3002 WHERE d_id = 7 AND d_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO orders1 (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (3001,7,1,2561,NOW(),12,1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client a */ INSERT INTO history1 (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data) VALUES (1,1,2786,10,1,NOW(),622,'name-zcsld name-mmsod ');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client a */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ INSERT INTO new_orders1 (no_o_id, no_d_id, no_w_id) VALUES (3001,7,1);",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 65 WHERE s_i_id = 42365 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,1,42365,1,1,7,'xxxxxxxxxxxxxxxxxxxxxxxx');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 53 WHERE s_i_id = 84125 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,2,84125,1,6,232,'nnnnnnnnnnnnnnnnnnnnnnnn');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 24 WHERE s_i_id = 29168 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,3,29168,1,8,372,'mmmmmmmmmmmmmmmmmmmmmmmm');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 31 WHERE s_i_id = 70752 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,4,70752,1,8,491,'llllllllllllllllllllllll');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 52 WHERE s_i_id = 3115 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,5,3115,1,8,60,'tttttttttttttttttttttttt');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 58 WHERE s_i_id = 60995 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,6,60995,1,9,334,'qqqqqqqqqqqqqqqqqqqqqqqq');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 23 WHERE s_i_id = 87468 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,7,87468,1,6,224,'gggggggggggggggggggggggg');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 45 WHERE s_i_id = 26507 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,8,26507,1,7,231,'zzzzzzzzzzzzzzzzzzzzzzzz');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 61 WHERE s_i_id = 38702 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,9,38702,1,6,300,'xxxxxxxxxxxxxxxxxxxxxxxx');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 38 WHERE s_i_id = 39823 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,10,39823,1,5,167,'mmmmmmmmmmmmmmmmmmmmmmmm');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 62 WHERE s_i_id = 53534 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,11,53534,1,7,380,'pppppppppppppppppppppppp');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ UPDATE stock1 SET s_quantity = 61 WHERE s_i_id = 62631 AND s_w_id= 1;",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1, Info: plan.UpdateInfo{Matched: 1, Updated: 1}}}},
			},
			{
				Query:    "/* client b */ INSERT INTO order_line1 (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info) VALUES (3001,7,1,12,62631,1,6,171,'gggggggggggggggggggggggg');",
				Expected: []sql.Row{{types.OkResult{RowsAffected: 1}}},
			},
			{
				Query:    "/* client b */ COMMIT;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client a */ SELECT * FROM dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
			{
				Query:    "/* client b */ SELECT * FROM dolt_constraint_violations;",
				Expected: []sql.Row{},
			},
		},
	})
}
