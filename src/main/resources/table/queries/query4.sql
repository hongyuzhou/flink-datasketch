select ss_sold_date_sk
     , hll(ss_item_sk)     as estimate_uniq_item_cnt
     , hll(ss_customer_sk) as estimate_uniq_customer_cnt
     , hll(ss_store_sk)    as estimate_uniq_store_cnt
from store_sales
group by ss_sold_date_sk

