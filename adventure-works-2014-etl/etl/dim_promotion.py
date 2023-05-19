"""
Transfer data from mssql dbo.DimPromotion to pgsql adventure_works_dwh.dim_promotion
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id="dim_promotion",
    start_date=datetime(2023, 1, 1),
    schedule=None
) as dag:

    @dag.task(task_id="read_dim_promotion")
    def read_dim_promotion():
        import os
        from avro_adapter import open_avro_writer

        def load_pandas_df():
            import os
            import pandas as pd
            dags_path = os.path.abspath(os.path.dirname(__file__))
            return pd.read_csv(f"{dags_path}/data/dim_promotion.csv")

        mssql_hook = MsSqlHook(mssql_conn_id="adventure_works_mssql", schema="AdventureWorks2014")
        path_ = os.path.abspath(os.path.dirname(__file__))
        records = mssql_hook.get_records(sql=open(f"{path_}/sql/get_dim_promotion.sql", "r").read())
        dict_keys = (
            "promotion_alternate_key",
            "english_promotion_name",
            "discount_pct",
            "english_promotion_type",
            "english_promotion_category",
            "start_date",
            "end_date",
            "min_qty",
            'max_qty'
        )
        df = load_pandas_df()
        keys_to_add = (
            "spanish_promotion_name",
            "french_promotion_name",
            "spanish_promotion_type",
            "french_promotion_type",
            "spanish_promotion_category",
            "french_promotion_category"
        )
        with open_avro_writer(identifier="promotion") as writer:
            for record in records:
                datum = dict(zip(dict_keys, record))
                datum["discount_pct"] = float(datum["discount_pct"])
                datum["start_date"] = datum["start_date"].timestamp()
                datum["end_date"] = datum["end_date"].timestamp()
                for k in keys_to_add:
                    datum[k] = df[k][datum["promotion_alternate_key"]]
                writer.append(datum)


    @dag.task(task_id="write_dim_promotion")
    def write_dim_promotion():
        pgsql_hook = PostgresHook(postgres_conn_id="adventure_works_pgsql", schema="adventure_works_dw_2014")
        import os
        from avro_adapter import open_avro_reader
        path_ = os.path.abspath(os.path.dirname(__file__))
        with open_avro_reader(identifier="promotion") as reader:
            for dst in reader:
                dst["start_date"] = datetime.fromtimestamp(dst["start_date"])
                dst["end_date"] = datetime.fromtimestamp(dst["end_date"])
                pgsql_hook.run(
                    sql=open(f"{path_}/sql/populate_dim_promotion.sql", "r").read(),
                    parameters=dst
                )


    read_dim_promotion()
    write_dim_promotion()
