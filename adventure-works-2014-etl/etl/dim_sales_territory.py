"""
Transfer data from mssql dbo.DimSalesTerritory to pgsql adventure_works_dwh.dim_sales_territory
"""
import os
from datetime import datetime

import avro.schema
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from avro.datafile import DataFileWriter, DataFileReader
from avro.io import DatumWriter, DatumReader

with DAG(
    dag_id="dim_sales_territory",
    start_date=datetime(2023, 1, 1),
    schedule=None
) as dag:

    @dag.task(task_id="read_dim_sales_territory")
    def read_dim_sales_territory():
        mssql_hook = MsSqlHook(mssql_conn_id="adventure_works_mssql", schema="AdventureWorksDW2014")
        records = mssql_hook.get_records(sql="select * from dbo.DimSalesTerritory")
        dags_path = os.path.abspath(os.path.dirname(__file__))
        schema = avro.schema.parse(open(f"{dags_path}/avro/sales_territory.avsc", "rb").read())
        writer = DataFileWriter(open(f"{dags_path}/avro/sales_territory.avro", "wb"), DatumWriter(), schema)
        for record in records:
            print(record)
            record_ = {
                "alternate_key": record[1],
                "region": record[2],
                "country": record[3],
                "territory_group": record[4]
            }
            if record[5] is not None:
                record_["territory_image"] = record[5]
            else:
                record_["territory_image"] = bytes()

            writer.append(record_)
        writer.close()


    @dag.task(task_id="write_dim_sales_territory")
    def write_dim_sales_territory():
        pgsql_hook = PostgresHook(postgres_conn_id="adventure_works_pgsql", schema="adventure_works_dw_2014")
        dags_path = os.path.abspath(os.path.dirname(__file__))
        reader = DataFileReader(open(f"{dags_path}/avro/sales_territory.avro", "rb"), DatumReader())
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
        reader.close()


    read_dim_sales_territory()
    write_dim_sales_territory()
