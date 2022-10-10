select hll_merge(ss_item_sk)     as estimate_uniq_item_cnt
     , hll_merge(ss_customer_sk) as estimate_uniq_customer_cnt
     , hll_merge(ss_hdemo_sk)    as estimate_uniq_hdemo_cnt
from store_sales

