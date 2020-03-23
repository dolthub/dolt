CREATE TABLE `mysql_results` (
  `test_file` VARCHAR(255) NOT NULL,
  `line_num` BIGINT NOT NULL,
  `duration` BIGINT NOT NULL,
  `query_string` LONGTEXT NOT NULL,
  `result` VARCHAR(255) NOT NULL,
  `error_message` LONGTEXT,
  `version` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`test_file`, `line_num`)
);
CREATE TABLE `mysql_mean_results` (
  `version` VARCHAR(255) NOT NULL,
  `test_file` VARCHAR(255) NOT NULL,
  `line_num` BIGINT NOT NULL,
  `mean_duration` DOUBLE NOT NULL,
  `result` LONGTEXT NOT NULL,
  PRIMARY KEY (`test_file`,`line_num`)
);
CREATE TABLE `nightly_dolt_results` (
  `test_file` VARCHAR(255) NOT NULL,
  `line_num` BIGINT NOT NULL,
  `duration` BIGINT NOT NULL,
  `query_string` LONGTEXT NOT NULL,
  `result` LONGTEXT NOT NULL,
  `error_message` LONGTEXT,
  `version` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`test_file`,`line_num`)
);
CREATE TABLE `nightly_dolt_mean_results` (
  `version` VARCHAR(255) NOT NULL,
  `test_file` VARCHAR(255) NOT NULL,
  `line_num` BIGINT NOT NULL,
  `mean_duration` DOUBLE NOT NULL,
  `result` LONGTEXT NOT NULL,
  PRIMARY KEY (`test_file`,`line_num`)
);
CREATE TABLE `releases_dolt_results` (
  `test_file` VARCHAR(255) NOT NULL,
  `line_num` BIGINT NOT NULL,
  `duration` BIGINT NOT NULL,
  `query_string` LONGTEXT NOT NULL,
  `result` LONGTEXT NOT NULL,
  `error_message` LONGTEXT,
  `version` VARCHAR(255) NOT NULL,
  PRIMARY KEY (`test_file`,`line_num`)
);
CREATE TABLE `releases_dolt_mean_results` (
  `version` VARCHAR(255) NOT NULL,
  `test_file` VARCHAR(255) NOT NULL,
  `line_num` BIGINT NOT NULL,
  `mean_duration` DOUBLE NOT NULL,
  `result` LONGTEXT NOT NULL,
  PRIMARY KEY (`test_file`,`line_num`)
);
create view `mysql_releases_duration_change`
as
select *
from
(
  select 
  m.test_file, 
  m.version as mysql_version, 
  sum(m.mean_duration) as mysql_mean_duration_sum_ms, 
  r.version as dolt_release_version, 
  sum(r.mean_duration) as dolt_release_mean_duration_sum_ms, 
  (100.0 * (cast(sum(m.mean_duration) as decimal(48, 16)) - 
    cast(sum(r.mean_duration) as decimal(48, 16))) / 
    (cast((sum(m.mean_duration)) as decimal(48, 16)) + .00001)) as percent_change 
  from mysql_mean_results as m 
  join releases_dolt_mean_results as r 
  on m.test_file = r.test_file and m.line_num = r.line_num
)
as wrapped where percent_change < -5.0;

create view `mysql_nightly_duration_change`
as
select *
from
(
  select 
  m.test_file, 
  m.version as mysql_version, 
  sum(m.mean_duration) as mysql_mean_duration_sum_ms, 
  n.version as dolt_nightly_version, 
  sum(n.mean_duration) as dolt_nightly_mean_duration_sum_ms, 
  (100.0 * (cast(sum(m.mean_duration) as decimal(48, 16)) - 
    cast(sum(n.mean_duration) as decimal(48, 16))) / 
    (cast((sum(m.mean_duration)) as decimal(48, 16)) + .00001)) as percent_change 
  from mysql_mean_results as m 
  join nightly_dolt_mean_results as n 
  on m.test_file = n.test_file and m.line_num = n.line_num
)
as wrapped where percent_change < -5.0;

create view `releases_nightly_duration_change`
as
select *
from
(
  select 
  r.test_file, 
  r.version as release_version, 
  sum(r.mean_duration) as release_mean_duration_sum_ms, 
  n.version as nightly_build, 
  sum(n.mean_duration) as nightly_mean_duration_sum_ms, 
  (100.0 * (cast(sum(r.mean_duration) as decimal(48, 16)) - 
    cast(sum(n.mean_duration) as decimal(48, 16))) / 
    (cast((sum(r.mean_duration)) as decimal(48, 16)) + .00001)) as percent_change 
  from releases_dolt_mean_results as r 
  join nightly_dolt_mean_results as n 
  on r.test_file = n.test_file and r.line_num = n.line_num
)
as wrapped where percent_change < -5.0;

create view `mysql_releases_result_change`
as 
select 
m.test_file, 
m.line_num, 
m.version as mysql_version, 
m.result as mysql_result, 
r.version as dolt_release_version, 
r.result as dolt_release_result 
from mysql_mean_results as m 
join releases_dolt_mean_results as r 
on m.line_num = r.line_num 
and m.test_file = r.test_file 
and (m.result = "ok" and r.result != "ok");

create view `mysql_nightly_result_change`
as 
select 
m.test_file, 
m.line_num, 
m.version as mysql_version, 
m.result as mysql_result, 
n.version as dolt_nightly_version, 
n.result as dolt_nightly_result 
from mysql_mean_results as m 
join nightly_dolt_mean_results as n 
on m.line_num = n.line_num 
and m.test_file = n.test_file 
and (m.result = "ok" and n.result != "ok");

create view `releases_nightly_result_change`
as 
select 
r.test_file, 
r.line_num, 
r.version as release_version, 
r.result as release_result, 
n.version as nightly_build, 
n.result as nightly_result 
from releases_dolt_mean_results as r 
join nightly_dolt_mean_results as n 
on r.line_num = n.line_num 
and r.test_file = n.test_file 
and (r.result = "ok" and n.result != "ok");
