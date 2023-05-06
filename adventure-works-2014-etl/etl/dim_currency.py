"""
Transfer data from mssql dbo.DimCurrency to pgsql adventure_works_dwh.dim_currency
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="dim_currency",
    start_date=datetime(2023, 1, 1),
    schedule=None
) as dag:

    @dag.task(task_id="read_dim_currency")
    def read_dim_currency():
        mssql_hook = MsSqlHook(mssql_conn_id="adventure_works_mssql", schema="AdventureWorksDW2014")
        records = mssql_hook.get_records(sql="select CurrencyAlternateKey, CurrencyName from dbo.DimCurrency")
        from avro_adapter import open_avro_writer
        with open_avro_writer(identifier="currency") as writer:
            for record in records:
                writer.append(dict(alternate_key=record[0], currency_name=record[1]))


    @dag.task(task_id="write_dim_currency")
    def write_dim_currency():
        pgsql_hook = PostgresHook(postgres_conn_id="adventure_works_pgsql", schema="adventure_works_dw_2014")
        from avro_adapter import open_avro_reader
        with open_avro_reader(identifier="currency") as reader:
            for dst in reader:
                pgsql_hook.run(
                    sql="""CALL public.populate_dim_currency( 
                      %(alternate_key)s, 
                      %(currency_name)s
                    )""",
                    parameters=dst
                )


    read_dim_currency()
    write_dim_currency()
