select round(avg(sql_mult),2) as avg
from (
    select
        o.test_name as test_name,
        o.detail,
        o.row_cnt,
        o.sorted as sorted,
        o.time as mysql_time,
        (
            select round((a.time / b.time),2) m
            from import_perf_results a
                     join import_perf_results b
                          on
                                      a.test_name = b.test_name and
                                      a.detail = b.detail
            where
                    a.server = 'dolt_server' and
                    b.server = 'mysql' and
                    a.test_name = o.test_name and
                    a.detail = o.detail
        ) as sql_mult,
        (
            select round((a.time / b.time),2) m
            from import_perf_results a
                     join import_perf_results b
                          on
                                      a.test_name = b.test_name and
                                      a.detail = b.detail
            where
                    a.server = 'dolt_cli' and
                    b.server = 'mysql' and
                    a.test_name = o.test_name and
                    a.detail = o.detail
        ) as cli_mult
    from import_perf_results as o
    where o.server = 'mysql'
    order by 1,2
) stats;
