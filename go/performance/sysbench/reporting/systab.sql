select detail, multiple
from (
  select
    detail,
    round(avg / lead(avg) over w, 2) as multiple,
    row_number() over w as rn
  from sysbench_results
  having mod(rn,2) = 1
  window w as (order by detail rows between 1 preceding and 1 following)
) sq;