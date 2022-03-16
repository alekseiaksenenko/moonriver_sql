with block_border as (
--     get bottom and ceil blocks
    select
        block_id
         ,lag(block_id) over () lag_block_id
         ,((event -> 'data')::jsonb ->> 1)::int as round
        ,row_number() over (order by block_id desc) as row_num
    from
        events
    where 1=1
        and network_id = 25
        and section = 'parachainStaking'
        and method = 'NewRound'
    limit
        3
    ),
--     1602000,,4426,1
--     1601400,1602000,4425,2
--     1600800,1601400,4424,3
--     23183

-- applying borders
apply_border as (
    select
        id
        ,block_id
        ,data
    from
        events
    where 1=1
      and network_id = 25
      and method = 'Rewarded'
      and block_id >= (
          select
                 block_id
          from
               block_border
          where row_num = (select max(row_num) from block_border)
          )
      and block_id < (
          select
                 block_id
          from
               block_border
          limit 1
          )
    ),

-- join account and amount
join_aacount_amount as (
    select
        a.block_id
        , bb.round
        , a.id
        , a.data::jsonb -> 0 ->> 'AccountId20' as account_id
        , ((a.data::jsonb -> 1 ->> 'u128') :: float) / 10^18 as amnt
    from
        apply_border a
        left join block_border bb
            on a.block_id < bb.lag_block_id
            and a.block_id >= bb.block_id
    )

select
       j.block_id
     , j.round
     , j.id
     , j.account_id
     , j.amnt
     , b.block_time
from
    join_aacount_amount j
    left join blocks b
        on j.block_id = b.id
where 1=1
    and b.network_id = 25


