with transactions_with_converted_amounts as (
    select * 
    from {{ref('int_netsuite__transactions_with_converted_amounts')}}
), 

--Below is only used if balance sheet transaction detail columns are specified dbt_project.yml file.
{% if var('balance_sheet_transaction_detail_columns') != []%}
transaction_details as (
    select * 
    from {{ ref('netsuite__transaction_details') }}
), 
{% endif %}

accounts as (
    select * 
    from {{ var('accounts') }}
), 

accounttypes as (
  select *
  from {{ var('accounttypes')}}
),

accounting_periods as (
    select * 
    from {{ var('accounting_periods') }}
), 

subsidiaries as (
    select * 
    from {{ var('subsidiaries') }}
),

balance_sheet as ( 
  select
    reporting_accounting_periods.accounting_period_id as accounting_period_id,
    reporting_accounting_periods.ending_at as accounting_period_ending,
    reporting_accounting_periods.name as accounting_period_name,
    reporting_accounting_periods.is_adjustment as is_accounting_period_adjustment,
    reporting_accounting_periods.is_closed as is_accounting_period_closed,
    transactions_with_converted_amounts.account_category as account_category,
    case
      when (accounttypes.is_balancesheet and reporting_accounting_periods.year_id = transaction_accounting_periods.year_id) then 'Net Income'
      when accounttypes.is_balancesheet then 'Retained Earnings'
      else accounts.account_name
        end as account_name,
    case
      when (accounttypes.is_balancesheet and reporting_accounting_periods.year_id = transaction_accounting_periods.year_id) then 'Net Income'
      when accounttypes.is_balancesheet then 'Retained Earnings'
      else accounttypes.type_name
        end as account_type_name,
    case
      when accounttypes.is_balancesheet then null
      else accounts.account_id
        end as account_id,
    case
      when accounttypes.is_balancesheet then null
      else accounts.account_number
        end as account_number,
    
    --The below script allows for accounts table pass through columns.
    {% if var('accounts_pass_through_columns') %}
    
    accounts.{{ var('accounts_pass_through_columns') | join (", accounts.")}} ,

    {% endif %}

    case
      when accounttypes.is_balancesheet or lower(transactions_with_converted_amounts.account_category) = 'equity' then -converted_amount_using_transaction_accounting_period
      when not accounttypes.is_leftside then -converted_amount_using_reporting_month
      when accounttypes.is_leftside then converted_amount_using_reporting_month
      else 0
        end as converted_amount,
    
    case
      when lower(accounttypes.type_name) = 'bank' then 1
      when lower(accounttypes.type_name) = 'accounts receivable' then 2
      when lower(accounttypes.type_name) = 'unbilled receivable' then 3
      when lower(accounttypes.type_name) = 'other current asset' then 4
      when lower(accounttypes.type_name) = 'fixed asset' then 5
      when lower(accounttypes.type_name) = 'other asset' then 6
      when lower(accounttypes.type_name) = 'deferred expense' then 7
      when lower(accounttypes.type_name) = 'accounts payable' then 8
      when lower(accounttypes.type_name) = 'credit card' then 9
      when lower(accounttypes.type_name) = 'other current liability' then 10
      when lower(accounttypes.type_name) = 'long term liability' then 11
      when lower(accounttypes.type_name) = 'deferred revenue' then 12
      when lower(accounttypes.type_name) = 'equity' then 13
      when (not accounttypes.is_balancesheet and reporting_accounting_periods.year_id = transaction_accounting_periods.year_id) then 15
      when not accounttypes.is_balancesheet then 14
      else null
        end as balance_sheet_sort_helper
    
    --Below is only used if balance sheet transaction detail columns are specified dbt_project.yml file.
    {% if var('balance_sheet_transaction_detail_columns') %}
    
    , transaction_details.{{ var('balance_sheet_transaction_detail_columns') | join (", transaction_details.")}}

    {% endif %}

  from transactions_with_converted_amounts
  
  --Below is only used if balance sheet transaction detail columns are specified dbt_project.yml file.
  {% if var('balance_sheet_transaction_detail_columns') != []%}
  join transaction_details
    on transaction_details.transaction_id = transactions_with_converted_amounts.transaction_id
      and transaction_details.transaction_line_id = transactions_with_converted_amounts.transaction_line_id
  {% endif %}


  join accounts 
    on accounts.account_id = transactions_with_converted_amounts.account_id

  left join accounttypes
    on accounts.account_type = accounttypes.accounttype_id

  join accounting_periods as reporting_accounting_periods 
    on reporting_accounting_periods.accounting_period_id = transactions_with_converted_amounts.reporting_accounting_period_id

  join accounting_periods as transaction_accounting_periods 
    on transaction_accounting_periods.accounting_period_id = transactions_with_converted_amounts.transaction_accounting_period_id

  where (accounttypes.is_balancesheet
      or transactions_with_converted_amounts.is_income_statement)

  union all

  select
    reporting_accounting_periods.accounting_period_id as accounting_period_id,
    reporting_accounting_periods.ending_at as accounting_period_ending,
    reporting_accounting_periods.name as accounting_period_full_name,
    reporting_accounting_periods.name as accounting_period_name,
    reporting_accounting_periods.is_adjustment as is_accounting_period_adjustment,
    reporting_accounting_periods.is_closed as is_accounting_period_closed,
    'Equity' as account_category,
    'Cumulative Translation Adjustment' as account_name,
    'Cumulative Translation Adjustment' as account_type_name,
    null as account_id,
    null as account_number,

    --The below script allows for accounts table pass through columns.
    {% if var('accounts_pass_through_columns') %}

    null as {{ var('accounts_pass_through_columns') | join (", null as ")}} ,

    {% endif %}

    case
      when lower(accounttypes.type_name) = 'equity' or transactions_with_converted_amounts.is_income_statement then transactions_with_converted_amounts.converted_amount_using_transaction_accounting_period
      else transactions_with_converted_amounts.converted_amount_using_reporting_month
        end as converted_amount,
    16 as balance_sheet_sort_helper

    --Below is only used if balance sheet transaction detail columns are specified dbt_project.yml file.
    {% if var('balance_sheet_transaction_detail_columns') %}

    , transaction_details.{{ var('balance_sheet_transaction_detail_columns') | join (", transaction_details.")}}

    {% endif %}

  from transactions_with_converted_amounts

  --Below is only used if balance sheet transaction detail columns are specified dbt_project.yml file.
  {% if var('balance_sheet_transaction_detail_columns') != []%}
  join transaction_details
    on transaction_details.transaction_id = transactions_with_converted_amounts.transaction_id
      and transaction_details.transaction_line_id = transactions_with_converted_amounts.transaction_line_id
  {% endif %}

  join accounts
    on accounts.account_id = transactions_with_converted_amounts.account_id
  
  left join accounttypes
    on accounts.account_type = accounttypes.accounttype_id
  
  join accounting_periods as reporting_accounting_periods 
    on reporting_accounting_periods.accounting_period_id = transactions_with_converted_amounts.reporting_accounting_period_id
    
  where 
    (accounttypes.is_balancesheet
      or transactions_with_converted_amounts.is_income_statement)
)

select *
from balance_sheet
