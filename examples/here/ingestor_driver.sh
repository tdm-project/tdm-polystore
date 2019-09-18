#!/bin/bash

app_id="DMWSuGha6KgsE1d9s2wJ"
app_code="voC1-vkRneenHHQTrpvR7A"
TIME=300

while true;
do
    python3 ingestor.py --app-id ${app_id} --app-code ${app_code} --source flow
    echo "Sleeping ${TIME}"
    sleep ${TIME}
done
