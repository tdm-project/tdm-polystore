DPC meteo data
==============

The following are instructions on how to run the dpc examples.
They apply **ONLY** to the startdev setup (make startdev).

Initialize database:
::
   docker-compose -f docker/docker-compose.yml exec web flask db init --drop

Clean up HDFS, just in case:
::
   docker-compose -f ./docker/docker-compose-dev.yml exec --user 0 tdmqc /bin/bash -c 'fake_user.sh hdfs dfs -rm -r /arrays'


Ingest data (for example the temperature time series):
::
   docker run --rm --network docker_default --user 1000:1000 --env NAMENODE_HOSTNAME=namenode tdmproject/dpc_ingestor -u http://web:8000 -v DEBUG temperature

