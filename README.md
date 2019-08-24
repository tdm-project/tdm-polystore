


Run the `TDMQ_API_Experiments.ipynb` notebook first to initialize the database
and load some data.


## Quick tips


### Build images

    make -j 4 images

### Start application

    make start


### Make just the `web` image

    make web

There are specific rules for all the images


### Restart the `web` service

    cd docker && docker-compose up -d --no-deps web

### Start the docker compose in development mode

    make startdev

### Run the tests, in the docker compose's tdmqc container

    docker-compose -f docker/docker-compose-dev.yml exec tdmqc tdmqc_run_tests
