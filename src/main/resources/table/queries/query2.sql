select ss_sold_date_sk
     , hll(ss_item_sk) as estimate_uniq_item_cnt
from store_sales
group by ss_sold_date_sk

