TDM Polystore
=============


General idea:

 * all objects are timeseries with annotations:
 * a timeseries is a collection of snapshots,
   * each snapshot has a timestamp and a geometrical footprint,
   * plus annotations and provenance information;
   * a geometrical footprint can go from a point to a volume;
 * use timescaledb + postgis to hold ghosts of the datasets
 * single shot events are 


Datacube query interface
------------------------

latitude = ( 4.5217, 4.5925)
longitude = (-71.7926, -71.6944)
product_class = "l7"
product_instance = "ledaps_meta_river"
ingestion_timestamp = xxxx

data = dc.load(latitude=latitude, longitude=longitude, product_class=,
               product_instance=,
               measurements = ['red', 'nir', 'pixel_qa'])

this will return a list of xarray(s)

use case sensor trace

dimensions  (latitude: 


with dims
print( gpm_data )

<xarray.Dataset>
Dimensions:               (latitude: 3, longitude: 3, time: 366)
Coordinates:
  * time                  (time) datetime64[ns] 2015-01-01T11:59:59.500000 ...
  * latitude              (latitude) float64 12.95 12.85 12.75
  * longitude             (longitude) float64 14.25 14.35 14.45
Data variables:
    total_precipitation   (time, latitude, longitude) int32 0 0 0 0 0 0 0 0 ...
    liquid_precipitation  (time, latitude, longitude) int32 0 0 0 0 0 0 0 0 ...
    ice_precipitation     (time, latitude, longitude) int32 0 0 0 0 0 0 0 0 ...
    percent_liquid        (time, latitude, longitude) uint8 15 15 15 15 15 ...
Attributes:
    crs:      EPSG:4326


 
