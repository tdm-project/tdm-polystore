PSWD=foobar
#IMAGE=timescale/timescaledb
IMAGE=timescale/timescaledb-postgis

# FIXME copying tests/data twice...
images:
	rm -rf docker/tdmq-dist ; mkdir docker/tdmq-dist
	cp -rf apidocs setup.py tdmq tests/data docker/tdmq-dist
	docker build -f docker/Dockerfile.tdmqc -t tdmproject/tdmqc docker
	docker build -f docker/Dockerfile.jupyter -t tdmproject/tdmqj docker
	docker build -f docker/Dockerfile.web -t tdmproject/tdmq docker

docker/docker-compose.yml: docker/docker-compose.yml-tmpl
	sed -e "s^LOCAL_PATH^$${PWD}^" -e "s^USER_UID^$$(id -u)^" \
            -e "s^USER_GID^$$(id -g)^"  \
            < docker/docker-compose.yml-tmpl > docker/docker-compose.yml

run: images docker/docker-compose.yml
	docker-compose -f ./docker/docker-compose.yml up

start: images docker/docker-compose.yml
	docker-compose -f ./docker/docker-compose.yml up -d

stop:
	docker-compose -f ./docker/docker-compose.yml down

clean: stop
