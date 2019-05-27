HERE TRAFFIC DOWNLOAD AND PROCESSING
------------------------------------

The bash script here_traffic.sh downloads every 5 minutes the whole traffic info for Sardinia and saves it
to a file named HERE_TRAFFIC_date-time.json.
The python script HERE_JSON_extractor.py downloads (if necessary) the OSM (driving) network of Sardinia and
saves it to disk. Each json file is read and processed to extract traffic info on each road. Each road is
characterized by a Position Code that we assign to a node of the OSM graph. These operations are cached to
speed up future processing. The final traffic info is encoded into a dictionary that is finally saved to disk. 

Be advised that each time the python script is run, the old traffic info is read from disk and then saved
with the updated snapshots. This file is about 1 GB for one month of recording.

HERE Traffic definition
-----------------------

https://developer.here.com/api-explorer/rest/traffic/traffic-flow-proximity

```
    "RWS" - A list of Roadway (RW) items
    "RW" = This is the composite item for flow across an entire roadway. A roadway item will be present for each roadway with traffic flow information available
    "FIS" = A list of Flow Item (FI) elements
    "FI" = A single flow item
    "TMC" = An ordered collection of TMC locations
    "PC" = Point TMC Location Code
    "DE" = Text description of the road
    "QD" = Queuing direction. '+' or '-'. Note this is the opposite of the travel direction in the fully qualified ID, For example for location 107+03021 the QD would be '-'
    "LE" = Length of the stretch of road. The units are defined in the file header
    "CF" = Current Flow. This element contains details about speed and Jam Factor information for the given flow item.
    "CN" = Confidence, an indication of how the speed was determined. -1.0 road closed. 1.0=100% 0.7-100% Historical Usually a value between .7 and 1.0
    "FF" = The free flow speed on this stretch of road.
    "JF" = The number between 0.0 and 10.0 indicating the expected quality of travel. When there is a road closure, the Jam Factor will be 10. As the number approaches 10.0 the quality of travel is getting worse. -1.0 indicates that a Jam Factor could not be calculated
    "SP" = Speed (based on UNITS) capped by speed limit
    "SU" = Speed (based on UNITS) not capped by speed limit
    "TY" = Type information for the given Location Referencing container. This may be freely defined string
```
