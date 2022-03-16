with block_border as (
    select
        block_id
    from
        events
    where 1=1
        and network_id = 25
        and section = 'parachainStaking'
        and method = 'NewRound'
    order by block_id desc
    limit
        2
    )

select
    *
    ,data::jsonb -> 0 -> 'AccountId20' as acc_id
    ,data::jsonb -> 1 ->> 'u128' as amnt_encode
    ,((data::jsonb -> 1 ->> 'u128') :: float) / 10^18 as amnt
from
     events
where 1=1
    and block_id >= (select block_id from block_border offset 1)
    and block_id < (select block_id from block_border limit 1)
--     and id = '1597800-68'
--     and id = '1600200-72'
    and method = 'Rewarded';
