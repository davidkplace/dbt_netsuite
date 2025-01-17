with transactions as (
    select * 
    from {{ var('transactions') }}
), 

transaction_lines as (
    select * 
    from {{ var('transaction_lines') }}
),
transaction_accounting_lines as (
  select *
  from {{ var('transaction_accounting_lines') }}
),

transaction_lines_w_accounting_period as ( -- transaction line totals, by accounts, accounting period and subsidiary
  select
    transaction_lines.transaction_id,
    transaction_lines.transaction_line_id,
    transaction_lines.subsidiary_id,
    transaction_accounting_lines.account_id,
    transactions.accounting_period_id as transaction_accounting_period_id,
    coalesce(transaction_accounting_lines.amount, 0) as unconverted_amount
  from transaction_lines

  join transactions on transactions.transaction_id = transaction_lines.transaction_id

  left join transaction_accounting_lines
    on transaction_lines.transaction_id = transaction_accounting_lines.transaction_id
    and transaction_lines.transaction_line_id = transaction_accounting_lines.transaction_line_id

  where 
    1 = 1
    --lower(transactions.transaction_type) != 'revenue arrangement' this needs to be adjusted to something like revarrang, but we dont use it so for now is ok
    and transaction_accounting_lines.is_posting
    --lower(non_posting_line) != 'yes'
)

select * 
from transaction_lines_w_accounting_period