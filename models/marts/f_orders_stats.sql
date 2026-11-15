{{
    config (
      engine='MergeTree()',
      order_by=['O_ORDERYEAR']
    )
}}
SELECT
    toYear(so.O_ORDERDATE ) AS O_ORDERYEAR
 	  ,so.O_ORDERSTATUS
    ,so.O_ORDERPRIORITY
    ,count(DISTINCT so.O_ORDERKEY) AS num_orders
    ,count(DISTINCT so.O_CUSTKEY) AS num_customers
    ,sum(sl.L_EXTENDEDPRICE * sl.L_DISCOUNT) AS revenue
FROM {{ ref('stg_orders') }} so  
inner join {{ ref('stg_lineitem') }} sl  on sl.`L_ORDERKEY`  = so.`O_ORDERKEY` 
GROUP BY
    toYear(so.O_ORDERDATE)
    , O_ORDERSTATUS
    , O_ORDERPRIORITY 
