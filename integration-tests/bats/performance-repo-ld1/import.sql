insert into jsonTable (
  with recursive cte (pk, j) as (
    select 0, JSON_OBJECT()
         union all
        select pk+1, JSON_INSERT(j, CONCAT("$.", pk), j) from cte where pk < 20
  ) select * from cte);
