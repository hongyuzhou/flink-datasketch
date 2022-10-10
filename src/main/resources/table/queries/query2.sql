select ss_store_sk
     , ss_promo_sk
     , count(distinct ss_item_sk)     as uniq_item_cnt
     , count(distinct ss_customer_sk) as uniq_customer_cnt
     , count(distinct ss_hdemo_sk)    as uniq_hdemo_cnt
from store_sales
group by ss_store_sk, ss_promo_sk