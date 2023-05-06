"""
Transfer data from mssql dbo.DimSalesTerritory to pgsql adventure_works_dwh.dim_sales_territory
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
    dag_id="dim_sales_territory",
    start_date=datetime(2023, 1, 1),
    schedule=None
) as dag:

    @dag.task(task_id="read_dim_sales_territory")
    def read_dim_sales_territory():
        mssql_hook = MsSqlHook(mssql_conn_id="adventure_works_mssql", schema="AdventureWorksDW2014")
        records = mssql_hook.get_records(sql="select * from dbo.DimSalesTerritory")
        from avro_adapter import open_avro_writer
        with open_avro_writer(identifier="sales_territory") as writer:
            for record in records:
                record_ = dict(alternate_key=record[1], region=record[2], country=record[3], territory_group=record[4])
                if record[5] is not None:
                    record_["territory_image"] = record[5]
                else:
                    record_["territory_image"] = bytes()

                writer.append(record_)


    @dag.task(task_id="write_dim_sales_territory")
    def write_dim_sales_territory():
        pgsql_hook = PostgresHook(postgres_conn_id="adventure_works_pgsql", schema="adventure_works_dw_2014")
        from avro_adapter import open_avro_reader
        with open_avro_reader(identifier="sales_territory") as reader:
            for dst in reader:
                pgsql_hook.run(
                    sql="""CALL public.populate_dim_sales_territory( 
                    %(alternate_key)s, 
                    %(region)s, 
                    %(country)s, 
                    %(territory_group)s,
                    %(territory_image)s)""",
                    parameters=dst
                )


    read_dim_sales_territory()
    write_dim_sales_territory()
