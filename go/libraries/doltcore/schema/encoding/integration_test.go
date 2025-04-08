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

package encoding_test

import (
	"context"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/types"
)

func TestSchemaSerializationIntegration(t *testing.T) {
	for i := range integrationTests {
		s := integrationTests[i].schema
		t.Run(getTestName(s), func(t *testing.T) {
			sch := parseSchemaString(t, s)
			t.Run("noms", func(t *testing.T) {
				testSchemaSerializationNoms(t, sch)
			})
			t.Run("flatbuffers", func(t *testing.T) {
				testSchemaSerializationFlatbuffers(t, sch)
			})
		})
	}
}

func testSchemaSerializationNoms(t *testing.T, sch schema.Schema) {
	ctx := context.Background()
	nbf := types.Format_Default
	vrw := getTestVRW(nbf)
	v, err := encoding.MarshalSchema(ctx, vrw, sch)
	require.NoError(t, err)
	s, err := encoding.UnmarshalSchema(ctx, nbf, v)
	require.NoError(t, err)
	assert.Equal(t, sch, s)
}

func testSchemaSerializationFlatbuffers(t *testing.T, sch schema.Schema) {
	ctx := context.Background()
	nbf := types.Format_Default
	vrw := getTestVRW(nbf)
	v, err := encoding.SerializeSchema(ctx, vrw, sch)
	require.NoError(t, err)
	s, err := encoding.DeserializeSchema(ctx, nbf, v)
	require.NoError(t, err)
	assert.Equal(t, sch, s)
}

func parseSchemaString(t *testing.T, s string) schema.Schema {
	ctx := context.Background()
	dEnv := dtestutils.CreateTestEnv()
	defer dEnv.DoltDB(ctx).Close()
	root, err := dEnv.WorkingRoot(ctx)
	require.NoError(t, err)
	eng, db, err := engine.NewSqlEngineForEnv(ctx, dEnv)
	require.NoError(t, err)
	sqlCtx, err := eng.NewDefaultContext(ctx)
	require.NoError(t, err)
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)
	sqlCtx.SetCurrentDatabase(db)
	_, sch, err := sqlutil.ParseCreateTableStatement(sqlCtx, root, eng.GetUnderlyingEngine(), s)
	require.NoError(t, err)
	return sch
}

func getTestVRW(nbf *types.NomsBinFormat) types.ValueReadWriter {
	ts := &chunks.TestStorage{}
	cs := ts.NewViewWithFormat(nbf.VersionString())
	return types.NewValueStore(cs)
}

func getTestName(sch string) string {
	n := sch[:strings.Index(sch, "(")]
	return strings.TrimSpace(n)
}

var integrationTests = []struct {
	schema string
}{
	{
		schema: "CREATE table t1 (" +
			"a INTEGER PRIMARY KEY check (a > 3)," +
			"b INTEGER check (b > a));",
	},
	{
		schema: "create table t2 (" +
			"pk int," +
			"c1 int," +
			"CHECK (c1 > 3)," +
			"PRIMARY KEY (pk));",
	},

	// SHAQ
	{
		schema: "CREATE TABLE `league_seasons` (" +
			"`league_id` int NOT NULL," +
			"`season_id` int NOT NULL," +
			"PRIMARY KEY (`league_id`,`season_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `leagues` (" +
			"`league_id` int NOT NULL," +
			"`name` varchar(100)," +
			"PRIMARY KEY (`league_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `player_season_stat_totals` (" +
			"`player_id` int NOT NULL," +
			"`team_id` int NOT NULL," +
			"`season_id` int NOT NULL," +
			"`minutes` int," +
			"`games_started` int," +
			"`games_played` int," +
			"`2pm` int," +
			"`2pa` int," +
			"`3pm` int," +
			"`3pa` int," +
			"`ftm` int," +
			"`fta` int," +
			"`ast` int," +
			"`stl` int," +
			"`blk` int," +
			"`tov` int," +
			"`pts` int," +
			"`orb` int," +
			"`drb` int," +
			"`trb` int," +
			"`pf` int," +
			"`season_type_id` int NOT NULL," +
			"`league_id` int NOT NULL DEFAULT 0," +
			"PRIMARY KEY (`player_id`,`team_id`,`season_id`,`season_type_id`,`league_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `players` (" +
			"`player_id` int NOT NULL," +
			"`nba_player_id` int," +
			"`date_of_birth` date," +
			"`first_name` varchar(255)," +
			"`last_name` varchar(255)," +
			"`height_inches` int," +
			"`weight_lb` int," +
			"PRIMARY KEY (`player_id`)," +
			"KEY `idx_last` (`first_name`,`last_name`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `season_types` (" +
			"`season_type_id` int NOT NULL," +
			"`description` varchar(55)," +
			"PRIMARY KEY (`season_type_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `team_seasons` (" +
			"`team_id` int NOT NULL," +
			"`league_id` int NOT NULL," +
			"`season_id` int NOT NULL," +
			"`prefix` varchar(100)," +
			"`nickname` varchar(100)," +
			"`abbreviation` varchar(100)," +
			"`city` varchar(100)," +
			"`state` varchar(100)," +
			"`country` varchar(100)," +
			"PRIMARY KEY (`team_id`,`league_id`,`season_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `teams` (" +
			"`team_id` int NOT NULL," +
			"`league_id` int NOT NULL," +
			"`full_name` varchar(100)," +
			"PRIMARY KEY (`team_id`,`league_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},

	// Sakila
	{
		schema: "CREATE TABLE `actor` (" +
			"`actor_id` smallint unsigned NOT NULL AUTO_INCREMENT," +
			"`first_name` varchar(45) NOT NULL," +
			"`last_name` varchar(45) NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`actor_id`)," +
			"KEY `idx_actor_last_name` (`last_name`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `address` (" +
			"`address_id` smallint unsigned NOT NULL AUTO_INCREMENT," +
			"`address` varchar(50) NOT NULL," +
			"`address2` varchar(50) DEFAULT NULL," +
			"`district` varchar(20) NOT NULL," +
			"`city_id` smallint unsigned NOT NULL," +
			"`postal_code` varchar(10) DEFAULT NULL," +
			"`phone` varchar(20) NOT NULL," +
			"`location` geometry NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`address_id`)," +
			"KEY `idx_fk_city_id` (`city_id`)," +
			"CONSTRAINT `fk_address_city` FOREIGN KEY (`city_id`) REFERENCES `city` (`city_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `all_types` (" +
			"`pk` int NOT NULL," +
			"`v1` binary(1) DEFAULT NULL," +
			"`v2` bigint DEFAULT NULL," +
			"`v3` bit(1) DEFAULT NULL," +
			"`v4` blob," +
			"`v5` char(1) DEFAULT NULL," +
			"`v6` date DEFAULT NULL," +
			"`v7` datetime DEFAULT NULL," +
			"`v8` decimal(5,2) DEFAULT NULL," +
			"`v9` double DEFAULT NULL," +
			"`v10` enum('s','m','l') DEFAULT NULL," +
			"`v11` float DEFAULT NULL," +
			"`v12` geometry DEFAULT NULL," +
			"`v13` int DEFAULT NULL," +
			"`v14` json DEFAULT NULL," +
			"`v15` linestring DEFAULT NULL," +
			"`v16` longblob," +
			"`v17` longtext," +
			"`v18` mediumblob," +
			"`v19` mediumint DEFAULT NULL," +
			"`v20` mediumtext," +
			"`v21` point DEFAULT NULL," +
			"`v22` polygon DEFAULT NULL," +
			"`v23` set('one','two') DEFAULT NULL," +
			"`v24` smallint DEFAULT NULL," +
			"`v25` text," +
			"`v26` time(6) DEFAULT NULL," +
			"`v27` timestamp DEFAULT NULL," +
			"`v28` tinyblob," +
			"`v29` tinyint DEFAULT NULL," +
			"`v30` tinytext," +
			"`v31` varchar(255) DEFAULT NULL," +
			"`v32` varbinary(255) DEFAULT NULL," +
			"`v33` year DEFAULT NULL," +
			"`v34` datetime(6) DEFAULT current_timestamp(6)," +
			"`v35` timestamp(6) DEFAULT now(6)," +
			"`v36` datetime(3) DEFAULT current_timestamp(3)," +
			"`v37` timestamp(3) DEFAULT now(3)," +
			"PRIMARY KEY (`pk`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `category` (" +
			"`category_id` tinyint unsigned NOT NULL AUTO_INCREMENT," +
			"`name` varchar(25) NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`category_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `city` (" +
			"`city_id` smallint unsigned NOT NULL AUTO_INCREMENT," +
			"`city` varchar(50) NOT NULL," +
			"`country_id` smallint unsigned NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`city_id`)," +
			"KEY `idx_fk_country_id` (`country_id`)," +
			"CONSTRAINT `fk_city_country` FOREIGN KEY (`country_id`) REFERENCES `country` (`country_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `country` (" +
			"`country_id` smallint unsigned NOT NULL AUTO_INCREMENT," +
			"`country` varchar(50) NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`country_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `customer` (" +
			"`customer_id` smallint unsigned NOT NULL AUTO_INCREMENT," +
			"`store_id` tinyint unsigned NOT NULL," +
			"`first_name` varchar(45) NOT NULL," +
			"`last_name` varchar(45) NOT NULL," +
			"`email` varchar(50) DEFAULT NULL," +
			"`address_id` smallint unsigned NOT NULL," +
			"`active` tinyint NOT NULL DEFAULT \"1\"," +
			"`create_date` datetime NOT NULL," +
			"`last_update` timestamp DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`customer_id`)," +
			"KEY `idx_fk_address_id` (`address_id`)," +
			"KEY `idx_fk_store_id` (`store_id`)," +
			"KEY `idx_last_name` (`last_name`)," +
			"CONSTRAINT `fk_customer_address` FOREIGN KEY (`address_id`) REFERENCES `address` (`address_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_customer_store` FOREIGN KEY (`store_id`) REFERENCES `store` (`store_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `film` (" +
			"`film_id` smallint unsigned NOT NULL AUTO_INCREMENT," +
			"`title` varchar(128) NOT NULL," +
			"`description` text," +
			"`release_year` year DEFAULT NULL," +
			"`language_id` tinyint unsigned NOT NULL," +
			"`original_language_id` tinyint unsigned DEFAULT NULL," +
			"`rental_duration` tinyint unsigned NOT NULL DEFAULT \"3\"," +
			"`rental_rate` decimal(4,2) NOT NULL DEFAULT \"4.99\"," +
			"`length` smallint unsigned DEFAULT NULL," +
			"`replacement_cost` decimal(5,2) NOT NULL DEFAULT \"19.99\"," +
			"`rating` enum('g','pg','pg-13','r','nc-17') DEFAULT \"G\"," +
			"`special_features` set('trailers','commentaries','deleted scenes','behind the scenes') DEFAULT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`film_id`)," +
			"KEY `idx_fk_language_id` (`language_id`)," +
			"KEY `idx_fk_original_language_id` (`original_language_id`)," +
			"KEY `idx_title` (`title`)," +
			"CONSTRAINT `fk_film_language` FOREIGN KEY (`language_id`) REFERENCES `language` (`language_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_film_language_original` FOREIGN KEY (`original_language_id`) REFERENCES `language` (`language_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `film_actor` (" +
			"`actor_id` smallint unsigned NOT NULL," +
			"`film_id` smallint unsigned NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`actor_id`,`film_id`)," +
			"KEY `idx_fk_film_id` (`film_id`)," +
			"CONSTRAINT `fk_film_actor_actor` FOREIGN KEY (`actor_id`) REFERENCES `actor` (`actor_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_film_actor_film` FOREIGN KEY (`film_id`) REFERENCES `film` (`film_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `film_category` (" +
			"`film_id` smallint unsigned NOT NULL," +
			"`category_id` tinyint unsigned NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`film_id`,`category_id`)," +
			"KEY `fk_film_category_category` (`category_id`)," +
			"CONSTRAINT `fk_film_category_category` FOREIGN KEY (`category_id`) REFERENCES `category` (`category_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_film_category_film` FOREIGN KEY (`film_id`) REFERENCES `film` (`film_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `film_text` (" +
			"`film_id` smallint NOT NULL," +
			"`title` varchar(255) NOT NULL," +
			"`description` text," +
			"PRIMARY KEY (`film_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `inventory` (" +
			"`inventory_id` mediumint unsigned NOT NULL AUTO_INCREMENT," +
			"`film_id` smallint unsigned NOT NULL," +
			"`store_id` tinyint unsigned NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`inventory_id`)," +
			"KEY `idx_fk_film_id` (`film_id`)," +
			"KEY `idx_store_id_film_id` (`store_id`,`film_id`)," +
			"CONSTRAINT `fk_inventory_film` FOREIGN KEY (`film_id`) REFERENCES `film` (`film_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_inventory_store` FOREIGN KEY (`store_id`) REFERENCES `store` (`store_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `language` (" +
			"`language_id` tinyint unsigned NOT NULL AUTO_INCREMENT," +
			"`name` char(20) NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`language_id`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `payment` (" +
			"`payment_id` smallint unsigned NOT NULL AUTO_INCREMENT," +
			"`customer_id` smallint unsigned NOT NULL," +
			"`staff_id` tinyint unsigned NOT NULL," +
			"`rental_id` int DEFAULT NULL," +
			"`amount` decimal(5,2) NOT NULL," +
			"`payment_date` datetime NOT NULL," +
			"`last_update` timestamp DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`payment_id`)," +
			"KEY `fk_payment_rental` (`rental_id`)," +
			"KEY `idx_fk_customer_id` (`customer_id`)," +
			"KEY `idx_fk_staff_id` (`staff_id`)," +
			"CONSTRAINT `fk_payment_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_payment_rental` FOREIGN KEY (`rental_id`) REFERENCES `rental` (`rental_id`) ON DELETE SET NULL ON UPDATE CASCADE," +
			"CONSTRAINT `fk_payment_staff` FOREIGN KEY (`staff_id`) REFERENCES `staff` (`staff_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `rental` (" +
			"`rental_id` int NOT NULL AUTO_INCREMENT," +
			"`rental_date` datetime NOT NULL," +
			"`inventory_id` mediumint unsigned NOT NULL," +
			"`customer_id` smallint unsigned NOT NULL," +
			"`return_date` datetime DEFAULT NULL," +
			"`staff_id` tinyint unsigned NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`rental_id`)," +
			"KEY `idx_fk_customer_id` (`customer_id`)," +
			"KEY `idx_fk_inventory_id` (`inventory_id`)," +
			"KEY `idx_fk_staff_id` (`staff_id`)," +
			"UNIQUE KEY `rental_date` (`rental_date`,`inventory_id`,`customer_id`)," +
			"CONSTRAINT `fk_rental_customer` FOREIGN KEY (`customer_id`) REFERENCES `customer` (`customer_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_rental_inventory` FOREIGN KEY (`inventory_id`) REFERENCES `inventory` (`inventory_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_rental_staff` FOREIGN KEY (`staff_id`) REFERENCES `staff` (`staff_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `staff` (" +
			"`staff_id` tinyint unsigned NOT NULL AUTO_INCREMENT," +
			"`first_name` varchar(45) NOT NULL," +
			"`last_name` varchar(45) NOT NULL," +
			"`address_id` smallint unsigned NOT NULL," +
			"`picture` blob," +
			"`email` varchar(50) DEFAULT NULL," +
			"`store_id` tinyint unsigned NOT NULL," +
			"`active` tinyint NOT NULL DEFAULT \"1\"," +
			"`username` varchar(16) NOT NULL," +
			"`password` varchar(40) collate utf8mb4_bin DEFAULT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`staff_id`)," +
			"KEY `idx_fk_address_id` (`address_id`)," +
			"KEY `idx_fk_store_id` (`store_id`)," +
			"CONSTRAINT `fk_staff_address` FOREIGN KEY (`address_id`) REFERENCES `address` (`address_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_staff_store` FOREIGN KEY (`store_id`) REFERENCES `store` (`store_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `store` (" +
			"`store_id` tinyint unsigned NOT NULL AUTO_INCREMENT," +
			"`manager_staff_id` tinyint unsigned NOT NULL," +
			"`address_id` smallint unsigned NOT NULL," +
			"`last_update` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP()," +
			"PRIMARY KEY (`store_id`)," +
			"KEY `idx_fk_address_id` (`address_id`)," +
			"UNIQUE KEY `idx_unique_manager` (`manager_staff_id`)," +
			"CONSTRAINT `fk_store_address` FOREIGN KEY (`address_id`) REFERENCES `address` (`address_id`) ON DELETE RESTRICT ON UPDATE CASCADE," +
			"CONSTRAINT `fk_store_staff` FOREIGN KEY (`manager_staff_id`) REFERENCES `staff` (`staff_id`) ON DELETE RESTRICT ON UPDATE CASCADE" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `types_default` (" +
			"`pk` int NOT NULL," +
			"`v1` binary(1) DEFAULT \"1\"," +
			"`v2` bigint DEFAULT \"1\"," +
			"`v3` bit(2) DEFAULT 2," +
			"`v4` blob DEFAULT (\"abc\")," +
			"`v5` char(1) DEFAULT \"i\"," +
			"`v6` date DEFAULT \"2022-02-22\"," +
			"`v7` datetime DEFAULT \"2022-02-22 22:22:22\"," +
			"`v8` decimal(5,2) DEFAULT \"999.99\"," +
			"`v9` double DEFAULT \"1.1\"," +
			"`v10` enum('s','m','l') DEFAULT \"s\"," +
			"`v11` float DEFAULT \"1.1\"," +
			"`v12` geometry DEFAULT (POINT(1, 2))," +
			"`v13` int DEFAULT \"1\"," +
			"`v14` json DEFAULT (JSON_OBJECT(\"a\", 1))," +
			"`v15` linestring DEFAULT (LINESTRING(POINT(0, 0),POINT(1, 2)))," +
			"`v16` longblob DEFAULT (\"abc\")," +
			"`v17` longtext DEFAULT (\"abc\")," +
			"`v18` mediumblob DEFAULT (\"abc\")," +
			"`v19` mediumint DEFAULT \"1\"," +
			"`v20` mediumtext DEFAULT (\"abc\")," +
			"`v21` point DEFAULT (POINT(1, 2))," +
			"`v22` polygon DEFAULT (POLYGON(LINESTRING(POINT(0, 0),POINT(8, 0),POINT(12, 9),POINT(0, 9),POINT(0, 0))))," +
			"`v23` set('one','two') DEFAULT \"one\"," +
			"`v24` smallint DEFAULT \"1\"," +
			"`v25` text DEFAULT (\"abc\")," +
			"`v26` time(6) DEFAULT \"11:59:59.000000\"," +
			"`v27` timestamp DEFAULT \"2021-01-19 03:14:07\"," +
			"`v28` tinyblob DEFAULT (\"abc\")," +
			"`v29` tinyint DEFAULT \"1\"," +
			"`v30` tinytext DEFAULT (\"abc\")," +
			"`v31` varchar(255) DEFAULT \"varchar value\"," +
			"`v32` varbinary(255) DEFAULT \"11111\"," +
			"`v33` year DEFAULT \"2018\"," +
			"PRIMARY KEY (`pk`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `collations` (" +
			"`pk` int NOT NULL," +
			"`v5` char(1) collate utf8mb3_esperanto_ci DEFAULT \"i\"," +
			"`v17` longtext collate utf8mb3_esperanto_ci DEFAULT (\"abc\")," +
			"`v20` mediumtext collate utf8mb3_esperanto_ci DEFAULT (\"abc\")," +
			"`v25` text collate utf8mb3_esperanto_ci DEFAULT (\"abc\")," +
			"`v31` varchar(255) collate utf8mb3_esperanto_ci DEFAULT \"varchar value\"," +
			"PRIMARY KEY (`pk`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_bin;",
	},
	{
		schema: "CREATE TABLE `collations2` (" +
			"`pk` int NOT NULL," +
			"`v5` char(1) DEFAULT \"i\"," +
			"`v17` longtext DEFAULT (\"abc\")," +
			"`v20` mediumtext collate utf8mb4_es_0900_ai_ci DEFAULT (\"abc\")," +
			"`v25` text collate utf8mb4_0900_bin DEFAULT (\"abc\")," +
			"`v31` varchar(255) collate utf8mb4_hungarian_ci DEFAULT \"varchar value\"," +
			"PRIMARY KEY (`pk`)" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_esperanto_ci;",
	},
}
