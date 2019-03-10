PSWD=foobar
#IMAGE=timescale/timescaledb
IMAGE=timescale/timescaledb-postgis

images:
	docker build -f docker/Dockerfile.hdfs -t tdm/hdfs docker
	docker build -f docker/Dockerfile.tiledb -t tdm/tiledb docker

run: images clean
	docker-compose -f ./docker/docker-compose.yml up

clean:
	docker-compose -f ./docker/docker-compose.yml down

