PSWD=foobar
#IMAGE=timescale/timescaledb
IMAGE=timescale/timescaledb-postgis

images:
	docker build -f docker/Dockerfile.hdfs -t tdm/hdfs docker
	docker build -f docker/Dockerfile.tiledb -t tdm/tiledb docker

run:
	docker run -d --name timescaledb \
                   -p 5432:5432 -e POSTGRES_PASSWORD=${PSWD} ${IMAGE}
