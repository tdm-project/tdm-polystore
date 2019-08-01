

PSWD=foobar

TDMQ_FILES=$(wildcard tdmq/*.py tdmq/client/*.py)

all: images

# FIXME copying tests/data twice...
docker/tdmq-dist: apidocs setup.py ${TDMQ_FILES} tests/data
	rm -rf docker/tdmq-dist ; mkdir docker/tdmq-dist
	cp -rf apidocs setup.py tdmq tests/data docker/tdmq-dist

tdmqc-deps: docker/Dockerfile.tdmqc
	docker build -f docker/Dockerfile.tdmqc  --target=deps -t tdmproject/tdmqc-deps docker

tdmqc: docker/tdmq-dist tdmqc-deps docker/Dockerfile.tdmqc
	docker build -f docker/Dockerfile.tdmqc -t tdmproject/tdmqc docker

jupyter: docker/tdmq-dist tdmqc-deps docker/Dockerfile.jupyter
	docker build -f docker/Dockerfile.jupyter -t tdmproject/tdmqj docker

web: docker/tdmq-dist docker/Dockerfile.web
	docker build -f docker/Dockerfile.web -t tdmproject/tdmq docker

images: tdmqc jupyter web

docker/docker-compose-dev.yml: docker/docker-compose.yml-tmpl
	sed -e "s^LOCAL_PATH^$${PWD}^" \
	    -e "s^USER_UID^$$(id -u)^" \
	    -e "s^USER_GID^$$(id -g)^" \
	    -e "s^DEV=false^DEV=true^" \
	    -e "s^#DEV *^^" \
	       < docker/docker-compose.yml-tmpl > docker/docker-compose-dev.yml


docker/docker-compose.yml: docker/docker-compose.yml-tmpl
	sed -e "s^LOCAL_PATH^$${PWD}^" \
	    -e "s^USER_UID^$$(id -u)^" \
	    -e "s^USER_GID^$$(id -g)^" \
	     < docker/docker-compose.yml-tmpl > docker/docker-compose.yml

run: images docker/docker-compose.yml
	docker-compose -f ./docker/docker-compose.yml up

startdev: images docker/docker-compose-dev.yml
	docker-compose -f ./docker/docker-compose-dev.yml up -d

stopdev:
	docker-compose -f ./docker/docker-compose-dev.yml down

start: images docker/docker-compose.yml
	docker-compose -f ./docker/docker-compose.yml up -d

stop:
	docker-compose -f ./docker/docker-compose.yml down

clean: stop


.PHONY: all tdmqc-deps tdmqc jupyter web images run start stop startdev stopdev clean
