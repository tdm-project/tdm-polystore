PSWD=foobar
#IMAGE=timescale/timescaledb
IMAGE=timescale/timescaledb-postgis
run:
	docker run -d --name timescaledb \
                   -p 5432:5432 -e POSTGRES_PASSWORD=${PSWD} ${IMAGE}
