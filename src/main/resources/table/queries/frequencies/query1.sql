select
    frequencies_items(t.customer_cnt) as top_k_list
from (
         select ss_store_sk
              , count(distinct ss_customer_sk) as customer_cnt
         from store_sales
         where ss_store_sk is not null
         group by ss_store_sk
     ) t
