SHELL := /bin/bash

# Set DOCKER_BUILD_EXTRA_ARGS argument to pass extra build arguments to Docker
# (e.g., --no-cache)

HADOOP_BASE_IMAGE=crs4/hadoopclient:3.2.1
TDMQ_FILES=$(wildcard tdmq/*.txt tdmq/*.py tdmq/client/*.py)

all: images

images: base-images

base-images: tdmqc jupyter web tdmq-db tdmq-client-conda tdmqc-conda

# FIXME copying tests/data twice...
docker/tdmq-dist: apidocs setup.py ${TDMQ_FILES} tests examples
	rm -rf docker/tdmq-dist ; mkdir docker/tdmq-dist
	cp -rf apidocs setup.py tdmq tests examples notebooks docker/tdmq-dist

tdmq-client-conda: docker/tdmq-dist docker/Dockerfile.tdmq-client-conda
	docker build $(DOCKER_BUILD_EXTRA_ARGS) -f docker/Dockerfile.tdmq-client-conda --target tdmq-client-conda -t tdmproject/tdmq-client-conda docker

tdmqc-conda: docker/tdmq-dist docker/Dockerfile.tdmq-client-conda
	docker build $(DOCKER_BUILD_EXTRA_ARGS) -f docker/Dockerfile.tdmq-client-conda --target tdmqc-conda -t tdmproject/tdmqc-conda docker

tdmq-client: docker/tdmq-dist docker/Dockerfile.tdmqc
	docker build $(DOCKER_BUILD_EXTRA_ARGS) \
		--build-arg HADOOP_BASE_IMAGE=${HADOOP_BASE_IMAGE}	\
		--build-arg HADOOP_CLASSPATH=$(shell docker run -it --rm ${HADOOP_BASE_IMAGE} /opt/hadoop/bin/hadoop classpath --glob) \
		-f docker/Dockerfile.tdmqc --target=tdmq-client -t tdmproject/tdmq-client docker

tdmqc: docker/tdmq-dist tdmq-client docker/Dockerfile.tdmqc
	docker build $(DOCKER_BUILD_EXTRA_ARGS) \
		--build-arg HADOOP_BASE_IMAGE=${HADOOP_BASE_IMAGE}	\
		--build-arg HADOOP_CLASSPATH=$(shell docker run -it --rm ${HADOOP_BASE_IMAGE} /opt/hadoop/bin/hadoop classpath --glob) \
		-f docker/Dockerfile.tdmqc -t tdmproject/tdmqc docker

jupyter: docker/tdmq-dist docker/Dockerfile.jupyter notebooks
	rm -rf docker/notebooks; cp -rf notebooks docker/notebooks
	docker build $(DOCKER_BUILD_EXTRA_ARGS) -f docker/Dockerfile.jupyter -t tdmproject/tdmqj docker

web: docker/tdmq-dist docker/Dockerfile.web
	docker build $(DOCKER_BUILD_EXTRA_ARGS) -f docker/Dockerfile.web -t tdmproject/tdmq docker

tdmq-db: docker/tdmq-db docker/tdmq-dist
	docker build $(DOCKER_BUILD_EXTRA_ARGS) -f docker/Dockerfile.tdmq-db -t tdmproject/tdmq-db docker

docker/docker-compose.dev.yml: docker/docker-compose.testing.yml-tmpl
	sed -e "s^LOCAL_PATH^$${PWD}^" \
	    -e "s^USER_UID^$$(id -u)^" \
	    -e "s^USER_GID^$$(id -g)^" \
	    -e "s^DEV=false^DEV=true^" \
	    -e "s^#DEV ^^" \
	       < docker/docker-compose.testing.yml-tmpl > docker/docker-compose.dev.yml


docker/docker-compose.testing.yml: docker/docker-compose.testing.yml-tmpl
	sed -e "s^LOCAL_PATH^$${PWD}^" \
	    -e "s^USER_UID^$$(id -u)^" \
	    -e "s^USER_GID^$$(id -g)^" \
	     < docker/docker-compose.testing.yml-tmpl > docker/docker-compose.testing.yml

run: base-images docker/docker-compose.base.yml docker/docker-compose.hdfs.yml docker/docker-compose.testing.yml
	docker-compose -f ./docker/docker-compose.base.yml -f docker/docker-compose.hdfs.yml -f docker/docker-compose.testing.yml up

startdev: base-images docker/docker-compose.base.yml docker/docker-compose.hdfs.yml docker/docker-compose.dev.yml
	chmod a+r ./docker/prometheus.yml
	docker-compose -f docker/docker-compose.base.yml -f docker/docker-compose.hdfs.yml -f docker/docker-compose.prometheus.yml -f docker/docker-compose.dev.yml up -d

stopdev: docker/docker-compose.base.yml docker/docker-compose.hdfs.yml docker/docker-compose.dev.yml
	docker-compose -f docker/docker-compose.base.yml -f docker/docker-compose.hdfs.yml -f docker/docker-compose.prometheus.yml -f docker/docker-compose.dev.yml down

start: base-images docker/docker-compose.base.yml docker/docker-compose.hdfs.yml docker/docker-compose.testing.yml
	chmod a+r ./docker/prometheus.yml
	docker-compose -f docker/docker-compose.base.yml -f docker/docker-compose.hdfs.yml -f docker/docker-compose.prometheus.yml -f docker/docker-compose.testing.yml up -d
	# Try to wait for timescaleDB and HDFS
	docker-compose -f ./docker/docker-compose.base.yml exec timescaledb bash -c 'for i in {1..8}; do sleep 5; pg_isready && break; done || { echo ">> Timed out waiting for timescaleDB" >&2; exit 2; }'
	docker-compose -f ./docker/docker-compose.hdfs.yml exec namenode hdfs dfsadmin -safemode wait
	docker-compose -f ./docker/docker-compose.hdfs.yml exec datanode bash -c 'for i in {1..8}; do sleep 5; datanode_cid && break; done || { echo ">> Timed out waiting for datanode to join HDFS" >&2; exit 3; }'

stop: docker/docker-compose.base.yml docker/docker-compose.hdfs.yml docker/docker-compose.testing.yml
	docker-compose -f docker/docker-compose.base.yml -f docker/docker-compose.hdfs.yml -f docker/docker-compose.prometheus.yml -f docker/docker-compose.testing.yml down

run-tests: start
	docker-compose -f ./docker/docker-compose.base.yml -f ./docker/docker-compose.hdfs.yml -f ./docker/docker-compose.testing.yml exec --user $$(id -u) tdmqc fake_user.sh /bin/bash -c 'cd $${TDMQ_DIST} && pytest -v tests'
	docker-compose -f ./docker/docker-compose.hdfs.yml exec namenode bash -c "hdfs dfs -mkdir -p /tiledb"
	docker-compose -f ./docker/docker-compose.hdfs.yml exec namenode bash -c "hdfs dfs -chmod a+wr /tiledb"
	docker-compose -f ./docker/docker-compose.base.yml -f ./docker/docker-compose.hdfs.yml -f ./docker/docker-compose.testing.yml exec tdmqj /bin/bash -c "python3 -c 'import tdmq, matplotlib'"
	docker-compose -f ./docker/docker-compose.base.yml -f ./docker/docker-compose.hdfs.yml -f ./docker/docker-compose.testing.yml exec tdmqj /bin/bash -c 'python3 $${TDMQ_DIST}/tests/quickstart_dense.py -f s3://quickdense/quickstart_array --log-level DEBUG'
	docker-compose -f ./docker/docker-compose.hdfs.yml exec namenode bash -c "hdfs dfs -rm -r hdfs://namenode:8020/tiledb"
	docker run -it --rm --net docker_tdmq --user $$(id -u) --env-file docker/settings.conf --env TDMQ_AUTH_TOKEN= tdmproject/tdmqc-conda /usr/local/bin/tdmqc_run_tests -k "not hdfs"


clean: stop
	rm -rf docker-stacks
	rm -rf docker/tdmq-dist
	rm -rf docker/notebooks

.PHONY: all tdmq-client-conda tdmqc-conda tdmq-client tdmqc jupyter web images base-images run start stop startdev stopdev clean
