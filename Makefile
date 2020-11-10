SHELL := /bin/bash

PSWD=foobar

TDMQ_FILES=$(wildcard tdmq/*.txt tdmq/*.py tdmq/client/*.py)

DOCKER_STACKS_REV := dc9744740e128ad7ca7c235d5f54791883c2ea69
# DOCKER_STACKS_REV := 3b1f4f5e6cc1fd81a14bd57e805fbb25daa3063c
TDMQJ_DEPS := tdmproject/tdmqj-deps
HADOOP_CLIENT_IMAGE := crs4/hadoopclient:3.2.0
NB_USER := tdm

all: images

images: base-images

base-images: tdmqc jupyter web tdmq-db

# FIXME copying tests/data twice...
docker/tdmq-dist: apidocs setup.py ${TDMQ_FILES} tests examples
	rm -rf docker/tdmq-dist ; mkdir docker/tdmq-dist
	cp -rf apidocs setup.py tdmq tests examples docker/tdmq-dist

tdmq-client: docker/tdmq-dist docker/Dockerfile.tdmqc
	docker build -f docker/Dockerfile.tdmqc --target=tdmq-client -t tdmproject/tdmq-client docker

tdmqc: docker/tdmq-dist tdmq-client docker/Dockerfile.tdmqc
	docker build -f docker/Dockerfile.tdmqc -t tdmproject/tdmqc docker

jupyter: docker/tdmq-dist docker/Dockerfile.jupyter notebooks
	cp -rf notebooks docker/notebooks
	docker build -f docker/Dockerfile.jupyter -t tdmproject/tdmqj docker

web: docker/tdmq-dist docker/Dockerfile.web
	docker build -f docker/Dockerfile.web -t tdmproject/tdmq docker

tdmq-db: docker/tdmq-db docker/tdmq-dist
	docker build -f docker/Dockerfile.tdmq-db -t tdmproject/tdmq-db docker

docker/docker-compose-dev.yml: docker/docker-compose.yml-tmpl
	sed -e "s^LOCAL_PATH^$${PWD}^" \
	    -e "s^USER_UID^$$(id -u)^" \
	    -e "s^USER_GID^$$(id -g)^" \
	    -e "s^DEV=false^DEV=true^" \
	    -e "s^#DEV ^^" \
	       < docker/docker-compose.yml-tmpl > docker/docker-compose-dev.yml


docker/docker-compose.yml: docker/docker-compose.yml-tmpl
	sed -e "s^LOCAL_PATH^$${PWD}^" \
	    -e "s^USER_UID^$$(id -u)^" \
	    -e "s^USER_GID^$$(id -g)^" \
	     < docker/docker-compose.yml-tmpl > docker/docker-compose.yml

run: base-images docker/docker-compose.yml
	docker-compose -f ./docker/docker-compose.yml up

startdev: base-images docker/docker-compose-dev.yml
	docker-compose -f ./docker/docker-compose-dev.yml up -d

stopdev:
	docker-compose -f ./docker/docker-compose-dev.yml down

start: base-images docker/docker-compose.yml
	docker-compose -f ./docker/docker-compose.yml up -d
	# Try to wait for timescaleDB and HDFS
	docker-compose -f ./docker/docker-compose.yml exec timescaledb bash -c 'for i in {{1..8}}; do sleep 5; pg_isready && break; done || { echo ">> Timed out waiting for timescaleDB" >&2; exit 2; }'
	docker-compose -f ./docker/docker-compose.yml exec namenode hdfs dfsadmin -safemode wait
	docker-compose -f ./docker/docker-compose.yml exec datanode bash -c 'for i in {{1..8}}; do sleep 5; datanode_cid && break; done || { echo ">> Timed out waiting for datanode to join HDFS" >&2; exit 3; }'

stop:
	docker-compose -f ./docker/docker-compose.yml down

run-tests: start
	docker-compose -f ./docker/docker-compose.yml exec --user $$(id -u) tdmqc fake_user.sh /bin/bash -c 'cd $${TDMQ_DIST} && pytest -v tests'
	docker-compose -f ./docker/docker-compose.yml exec namenode bash -c "hdfs dfs -mkdir /tiledb"
	docker-compose -f ./docker/docker-compose.yml exec namenode bash -c "hdfs dfs -chmod a+wr /tiledb"
	docker-compose -f ./docker/docker-compose.yml logs tdmqj-hub
	docker-compose -f ./docker/docker-compose.yml exec tdmqj-hub bash -c "sed -i s/localhost/namenode/ /opt/hadoop/etc/hadoop/core-site.xml"
	docker-compose -f ./docker/docker-compose.yml exec --user $$(id -u) tdmqj-hub fake_user.sh /bin/bash -c "python /quickstart_dense.py -f hdfs://namenode:8020/tiledb"
	docker-compose -f ./docker/docker-compose.yml exec namenode bash -c "hdfs dfs -rm -r hdfs://namenode:8020/tiledb"
	docker-compose -f ./docker/docker-compose.yml exec tdmqj-hub bash -c "python -c 'import tdmq, matplotlib'"



clean: stop
	rm -rf docker-stacks

.PHONY: all tdmqc-deps tdmqc jupyter web images base-images run start stop startdev stopdev clean
