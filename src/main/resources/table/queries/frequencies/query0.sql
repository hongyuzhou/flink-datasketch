select ss_store_sk
     , customer_cnt
from (
         select ss_store_sk
              , customer_cnt
              , row_number() over(order by customer_cnt desc) as rn
         from (
                  select ss_store_sk
                       , count(distinct ss_customer_sk) as customer_cnt
                  from store_sales
                  where ss_store_sk is not null
                  group by ss_store_sk
              )
     ) t
where t.rn <= 5
order by t.rn
