LOCAL_AIRFLOW_HOME := airflow

.phony: dev_deploy
dev_deploy:
	set -ev
	mkdir -p $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/avro
	cp etl/avro/*.avsc $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/avro/
	cp etl/*.py $${HOME}/${LOCAL_AIRFLOW_HOME}/dags/

