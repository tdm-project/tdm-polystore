TDMQ data model
===============

Logical view
------------

Sources
^^^^^^^

A source could be:
 * a real source that acquire real world data, e.g., a temperature sensor;
 * a virtual source that integrates real world data, e.g., a mosaic of
   meteo radar data;
 * a virtual source that generates virtual data, e.g., simulation
   results (that is, we have a virtual sensor that reads the result of
   a fact that happened in a virtual world).

All sources have a set of common properties:
 * an **id**, e.g., 'edge-station-00033/foo',
   'tdm/wrf-4.1:<unique-run-id>', this is a unique identifier for the
   source;
 * an **alias**, e.g., 'edge-station-sala-macchine', this will be the
   printable name for the source;
 * an **entity type** code, e.g., 'WeatherObserver', 'TrafficFlowSensor', which is defined along with an entity category (e.g., ‘Station’, ‘Radar’);
 * a **geometrical footprint type**, e.g., Point, Surface, Volume (also
   sequences of geometrical footprints, e.g., a moving sensor
   acquisition-positions)
 * a list of **controlledProperties**, e.g., [temperature, humidity]

   
Source could also have other, EntityType specific, properties.

A source acquires a timeseries of geo-located(?) datasets.

Geo-located records
^^^^^^^^^^^^^^^^^^^

A geo-located record is defined(?) by:
 * a UTC timestamp;
 * a geometrical footprint;
 * a dataset.

A dataset could be (at the moment):
 * a single scalar;
 * a multi-dimensional array;
 * a dictionary of the above;

Possible footprint scenarios:
 - a point (a GeoJson Point)
   - e.g., a temperature pt sensor
 - a surface (described by its boundary, expressed as a GeoJson Polygon)
   - e.g., a meteo radar snapshot, a meteo-simulation result (e.g., UV10m)
 - a volume (described by its bounding (flat) surfaces, expressed as a
   GeoJson MultiPolygon)
   - e.g., a 3D simulation result
 - a path (that is, different footprints at different timesteps)
   - e.g., a satellite acquisition strip, a moving sensor acquisition

The timeseries acquisition could be open-ended (e.g., weather stations
sensors) or limited (i.e., a simulation).


Data access
-----------

Data is always selected and accessed via the TDMQ API.


Query
^^^^^

Select sources
""""""""""""""

The basic query operation is asking TDMQ the list of sources for which
is true a logical function (and/or?) on the following predicates:

 * it has  measures taken in a given spatio-temporal cylinder;
 * it satisfies given constraints on its properties.

Example:
::
    from tdmq.client.client import Client
    c = Client()
    after = 
    before = 
    args = {
        'footprint': 'circle((9.2215, 30.0015), 1000000)',
        'after': '2019-01-01T00:00:00Z',
        'before': '2099-12-31T23:59:59Z'
    }
    sources = c.get_sources(args)


Once the sources have been selected, actual data can be accessed as
timeseries.

Get timeseries
""""""""""""""

TDMQ supports querying a source for a timeseries derived from its
data.  The request could be constrained by specifying a time interval
and, optionally, a subset of the properties controlled by the source.

Moreover, the timeseries request could also specify a time bucket
duration -- e.g., 5 minutes -- and an operation to be performed on the
bucketed data, e.g., return a timeseries obtained by averaging the
data of the original timeseries every 5 minutes.

FIXME We do not specify if the time labels of the derived timeseries
is at the beginning/middle/end of the time buckets.

FIXME no discussion of actual data access

Example:
::
    args = {
        'after': after,
        'before': before,
    }
    ts = t_source.timeseries(**args)
    (tdeltas, data) = ts[0:100, 300:440, 100:200]
    N = 10
    temp = data['TEMP']
    fig, axes = plt.subplots(N//5, 5)
    for i in range(N):
        ax = axes[i//5, i%5]
        ax.imshow(temp[i] * (temp[i] > -9000.0))
        ax.set_title('%d' % tdeltas[i])


Ingestion
^^^^^^^^^

A measure should contain, at a minimum, the following information:
 * a source id;
 * a timestamp, UTC;
 * a geometry;
 * a dataset, this could either be an actual dataset (e.g.,
   `{'temperature': 23.0, 'humidity': 45.0}`) or a reference to one
   (e.g., `{'uri': 'hdfs://storage.tdmq.it/arrays/<uuid>', 'index': 33}`)

The difficult thing is 





FIXME describe logical operation


Implementation
--------------

Source description
^^^^^^^^^^^^^^^^^^
FIXME describe the JSON description of a source, and then specific


Measure description
^^^^^^^^^^^^^^^^^^^



REST API
^^^^^^^^

Sources description
^^^^^^^^^^^^^^^^^^^


General strategy, use NGSI ontologies whenever possible.

Things a source should have:

 * an id, e.g., 'edge-station-00033/foo', 'tdm/wrf-4.1:<unique-run-id>';
 * a name, e.g., 'edge-station-sala-macchine';
 * an EntityType;
 * a geometrical footprint [should we move this to the measure?]
 * a list of controlledProperties
 * other 

temperature, humidity, light, motion, fillingLevel, occupancy, power,
pressure, smoke, energy, airPollution, noiseLevel, weatherConditions,
precipitation, windSpeed, windDirection, atmosphericPressure,
solarRadiation, depth, pH, conductivity, conductance, tss, tds,
turbidity, salinity, orp, cdom, waterPollution, location, speed,
heading, weight, waterConsumption, gasComsumption,
electricityConsumption, soilMoisture, trafficFlow, eatingActivity,
milking, movementActivity


Available source entityType(s)
""""""""""""""""""""""""""""""

 * <electricitySource>
 * TrafficFlowSensor
 * WeatherForecaster
 * WeatherObserver
 * LandObserver









   
Geometry footprint description
""""""""""""""""""""""""""""""


Measures description
^^^^^^^^^^^^^^^^^^^^

Things a measure should have:

 * a timestamp, UTC
 * a geometry [what happens if it is repeated?]


Available measure EntityType(s)
"""""""""""""""""""""""""""""""

 * TrafficFlowObserved observed by a TrafficFlowSensor
 * WeatherForecast made by a WeatherForecaster
 * WeatherObserved observed by WeatherObserver
 * LandObserved observed by LandObserver


Data management
"""""""""""""""


temperature, humidity, light, motion, fillingLevel,  occupancy, power, pressure, smoke, energy, airPollution,  noiseLevel, weatherConditions, precipitation, windSpeed,  windDirection, atmosphericPressure, solarRadiation, depth, pH,  conductivity, conductance, tss, tds, turbidity, salinity,  orp, cdom, waterPollution, location, speed, heading,  weight, waterConsumption, gasComsumption,  electricityConsumption, soilMoisture, trafficFlow,  eatingActivity, milking, movementActivity



TrafficFlowObserved observed by ?

Reported observations:

intensity : Total number of vehicles detected during this observation period.
Attribute type: Property. Number. Positive integer.
Optional

occupancy : Fraction of the observation time where a vehicle has been occupying the observed laned.
Attribute type: Property. Number between 0 and 1.
Optional

averageVehicleSpeed : Average speed of the vehicles transiting during the observation period.
Attribute type: Property. Number
Default unit: Kilometer per hour (Km/h).
Optional


averageVehicleLength : Average length of the vehicles transiting during the observation period.
Attribute type: Property. Number
Default unit: meter (m)
Optional

congested : Flags whether there was a traffic congestion during the observation period in the referred lane. The absence of this attribute means no traffic congestion.
Attribute type: Property. Boolean
Optional

averageHeadwayTime : Average headway time. Headaway time is the time ellapsed between two consecutive vehicles.
Attribute type: Property. Number
Default unit: second (s)
Optional

averageGapDistance : Average gap distance between consecutive vehicles.
Attribute type: Property. Number
Default unit: meter (m)
Optional

laneDirection : Usual direction of travel in the lane referred by this observation. This attribute is useful when the observation is not referencing any road segment, allowing to know the direction of travel of the traffic flow observed.
Attribute type: Property. Text
Allowed values: (forward, backward). See RoadSegment.laneUsage for a description of the semantics of these values.
Optional

reversedLane: Flags whether traffic in the lane was reversed during the observation period. The absence of this attribute means no lane reversion.


Attribute type: Property. Boolean

































WeatherForecast made by?



controlledProperties:

temperature, humidity, light, motion, fillingLevel,  occupancy, power, pressure, smoke, energy, airPollution,  noiseLevel, weatherConditions, precipitation, windSpeed,  windDirection, atmosphericPressure, solarRadiation, depth, pH,  conductivity, conductance, tss, tds, turbidity, salinity,  orp, cdom, waterPollution, location, speed, heading,  weight, waterConsumption, gasComsumption,  electricityConsumption, soilMoisture, trafficFlow,  eatingActivity, milking, movementActivity






