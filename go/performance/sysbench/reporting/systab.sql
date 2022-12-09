select name, mean_mult, med_mult, stdd
from (
  select
    trim(TRAILING '.gen.lua' FROM trim(LEADING 'gen/' FROM test_name)) as name,
    round(avg / lead(avg) over w, 2) as mean_mult,
    round(median / lead(median) over w, 2) as med_mult,
    round(sqrt(power(first_value(stdd) over w, 2) + power(last_value(stdd) over w, 2)), 3) as stdd,
    row_number() over w     as rn
  from sysbench_results
  having mod(rn,2) = 1
  window w as (order by test_name rows between CURRENT ROW and 1 following)
) sq;
