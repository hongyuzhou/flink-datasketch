select ss_store_sk
     , cpc(ss_item_sk)     as estimate_uniq_item_cnt
     , cpc(ss_customer_sk) as estimate_uniq_customer_cnt
     , cpc(ss_hdemo_sk)    as estimate_uniq_hdemo_cnt
from store_sales
group by ss_store_sk