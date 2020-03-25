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
