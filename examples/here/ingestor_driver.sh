#!/bin/bash

app_id="NOT_A_GOOD_ID"
app_code="NOT_A_GOOD_APP_CODE"
TIME=300

while true;
do
    echo "Sleeping ${TIME}"
    sleep ${TIME}
    python3 ingestor.py --app-id ${app_id} --app-code ${app_code} --source flow    
done


