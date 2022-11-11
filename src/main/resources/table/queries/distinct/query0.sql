select count(distinct ss_item_sk)     as uniq_item_cnt
     , count(distinct ss_customer_sk) as uniq_customer_cnt
     , count(distinct ss_hdemo_sk)    as uniq_hdemo_cnt
from store_sales
