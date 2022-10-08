select ss_sold_date_sk
     , ss_store_sk
     , count(distinct ss_item_sk)     as uniq_item_cnt
     , count(distinct ss_customer_sk) as uniq_customer_cnt
     , count(distinct ss_store_sk)    as uniq_store_cnt
from store_sales
group by ss_sold_date_sk, ss_store_sk