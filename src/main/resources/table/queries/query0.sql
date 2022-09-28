select ss_sold_date_sk
     , count(distinct ss_item_sk) as uniq_item_cnt
from store_sales
group by ss_sold_date_sk
