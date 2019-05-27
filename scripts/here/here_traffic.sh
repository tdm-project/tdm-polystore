#!/bin/bash
while true
do
	echo "Downloading"
	curl 'https://traffic.api.here.com/traffic/6.2/flow.json?app_id=DMWSuGha6KgsE1d9s2wJ&app_code=voC1-vkRneenHHQTrpvR7A&bbox=41.273966,8.034185;38.800759,9.836935&criticality=minor' > "HERE_TRAFFIC_$(date +%Y-%m-%d_%H:%M).json"
	sleep 300
done

