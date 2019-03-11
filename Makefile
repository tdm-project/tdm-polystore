PSWD=foobar
#IMAGE=timescale/timescaledb
IMAGE=timescale/timescaledb-postgis

images:
	docker build -f docker/Dockerfile.hdfs -t tdm/hdfs docker
	docker build -f docker/Dockerfile.tiledb -t tdm/tiledb docker


run: images clean
	#if [ ${UID}!=1000 ]; then echo "UID != 1000, the notebook directory will not be writable" 
	sed -r "s^LOCAL_PATH^${PWD}^"  < docker/docker-compose.yml-tmpl > docker/docker-compose.yml
	docker-compose -f ./docker/docker-compose.yml up

clean:
	docker-compose -f ./docker/docker-compose.yml down

