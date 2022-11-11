select ss_item_sk
     , ss_sales_price
from (
         select ss_item_sk
              , ss_sales_price
              , row_number() over(order by ss_sales_price desc) as rn
         from (
                  select ss_item_sk
                       , sum(ss_sales_price) as ss_sales_price
                  from store_sales
                  group by ss_item_sk
              )
     ) t
where t.rn <= 10
