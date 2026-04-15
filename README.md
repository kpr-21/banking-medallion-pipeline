# Banking Medallion Data Pipeline

End-to-end data pipeline using Airflow, PySpark, and Delta Lake implementing Bronze, Silver, Gold architecture.

             generate_data
                    |
     ---------------------------------
     |               |               |
wait_customers   wait_accounts   wait_transactions
     |               |               |
customers_parq   accounts_parq   transactions_parq
“We process all files for a given process_date and overwrite that partition to maintain idempotency. This assumes that all required files for that batch are available at processing time. In production, we would ensure completeness via file validation or upstream guarantees.”