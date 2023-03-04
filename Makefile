MSSQL_DB := mssql-db
MSSQL_NET := mssql-net
PGSQL_DB := pgsql-db
PGSQL_NET := pgsql-net

.phony: mssql_volume
mssql_volume:
	@[ -z "$(docker volume ls | grep ${MSSQL_DB})" ] || docker volume create ${MSSQL_DB}

.phony: mssql_net
mssql_net:
	@[ -z "$(docker network ls | grep ${MSSQL_NET})" ] || docker network create ${MSSQL_NET}

.phony: mssql
mssql: mssql_volume mssql_net
	@docker run \
	-e "ACCEPT_EULA=Y" \
	-e "MSSQL_SA_PASSWORD=Frobbing@123" \
	--network ${MSSQL_NET} \
	--name mssql1 \
	--hostname mssql1 \
	--mount "type=volume,src=${MSSQL_DB},dst=/var/opt/mssql" \
	-v "$${PWD}/data-dumps:/data" \
	-d \
	--rm \
	mcr.microsoft.com/mssql/server:2022-latest

.phony: pgsql_volume
pgsql_volume:
	@[ -z "$(docker volume ls | grep ${PGSQL_DB})" ] || docker volume create ${PGSQL_DB}

.phony: pgsql_net
pgsql_net:
	@[ -z "$(docker network ls | grep ${PGSQL_NET})" ] || docker network create ${PGSQL_NET}

.phony: pgsql
pgsql: pgsql_volume pgsql_net
	@docker run \
	-d \
	--rm \
	--name pgsql1 \
	--hostname pgsql1 \
	-e POSTGRES_PASSWORD=xdrf@123 \
	--mount "type=volume,src=${PGSQL_DB},dst=/var/lib/postgresql/data" \
	--network ${PGSQL_NET} \
	postgres:15.1-bullseye
