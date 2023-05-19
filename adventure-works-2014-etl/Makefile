LOCAL_AIRFLOW_HOME := airflow
LOCAL_AVRO_DIR := avro
LOCAL_SQL_DIR := sql
LOCAL_DATA_DIR := data

.phony: local_data_dir
local_data_dir:
	@mkdir -p $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/${LOCAL_DATA_DIR}

.phony: local_avro_dir
local_avro_dir:
	@mkdir -p $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/${LOCAL_AVRO_DIR}

.phony: local_sql_dir
local_sql_dir:
	@mkdir -p $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/${LOCAL_SQL_DIR}

.phony: dev_deploy
dev_deploy: local_avro_dir local_sql_dir local_data_dir
	@set -ev
	@cp etl/avro/*.avsc $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/${LOCAL_AVRO_DIR}/
	@cp etl/sql/*.sql $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/${LOCAL_SQL_DIR}/
	@cp etl/data/*.csv $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/${LOCAL_DATA_DIR}/
	@cp etl/*.py $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/
