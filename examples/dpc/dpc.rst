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


Install missing modules:
::
   docker-compose -f ./docker/docker-compose-dev.yml exec --user 0  tdmqc /bin/bash -c 'cd ${TDMQ_DIST}/tests && fake_user.sh pip3 install tiffile' 


Now run setup:
::
   docker-compose -f ./docker/docker-compose-dev.yml exec --user $(id -u) tdmqc /bin/bash -c 'cd ${TDMQ_DIST}/examples/dpc && fake_user.sh python3 setup_source.py --source temperature' 

And now we can have an AirFlow cronjob that will ingest new data
::
   docker-compose -f ./docker/docker-compose-dev.yml exec --user $(id -u) tdmqc /bin/bash -c 'cd ${TDMQ_DIST}/examples/dpc && fake_user.sh python3 ingestor.py --source temperature'

   
  667  docker-compose -f ./docker/docker-compose-dev.yml exec --user $(id -u) tdmqc /bin/bash -c 'cd ${TDMQ_DIST}/tests && fake_user.sh bash -l' 
