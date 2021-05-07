
# Understanding sensor data in the TDM polystore

## High-level model

The TDM polystore has a very simple data model with two main entities:  Sources
and Timeseries.

A Source is some data-producing sensor.  It produces data records, which are
ingested by the platform and made available as Timeseries -- which represent
a specific query on the available data.  Examples of Sources are the Current
Transformers (CTs) attached to a [IotaWatt energy
monitor](https://iotawatt.com/), or the photosensitive pulse counter on an
[EmonTX](https://guide.openenergymonitor.org/technical/emontx/), or a doppler
weather radar.


<img alt="Polystore data model" src="images/API_model.png" width="500" />


## The Source

All sources have a set of properties that serve to identify and describe them. 

We distinguish sources it two broad categories:  **Scalar Sources** and **Non-scalar
sources**.

*Scalar Sources* are defined by the fact that they have an empty `shape` vector.
At a practical level, a time series produced by a scalar sources is an array of
scalar values.

On the other hand, the time series produced by *Non-scalar Sources* are sequences of
arrays of any number of dimensions greater than 0.  The `shape` property of the
Source will describe the dimensionality of each individual timestamped data
point recorded from the Source.

For specific information about these two broad categories consult the dedicated
pages:
* [scalar sources](scalar-metadata)
* [non-scalar sources](multidimensional-metadata)


### Identifying a Source

Every source is identified by a unique `tdmq_id`.  This ID can be used to
retrieve a specific, pre-identified Source with the Client module or through the
REST API.


### What type of device is it?

The type of Source and the time series it generates is described by the
combination of several properties, which serve to both understand what
type of Source was returned by a query and also to query the platform for the
specific sensors that you require.

1. The type of Source classified by its `entity_category` and `entity_type`.
2. The specific Source may specify additional metadata describing it (more on
   this later).
3. The Source will have specific list of *controlledProperties* listing its
   capabilities.

Note that a Source may not transmit data for all its theoretical capabilities.

#### Entity categories and entity types

Every source represents an entity with a specific category and type; this model
is inspired by the FIWARE data model.  Within each category, each entity type is
unique.  On the other hand, the same type *name* may appear in two different
categories, though the two entity types would not be related in any way.

These are the currently defined entity categories and types.

| Category   | Type                     |
| ---        | ---                      |
| Radar      | MeteoRadar               |
| Radar      | MeteoRadarMosaic         |
| Satellite  |                          |
| Simulation |                          |
| Station    | WeatherObserver          |
| Station    | EnergyConsumptionMonitor |
| Station    | TrafficObserver          |
| Station    | DeviceStatusMonitor      |

:bulb: Throughout this document we use the syntax
`entity_category`**::**`entity_type`; e.g., *Station::EnergyConsumptionMonitor*.

## General Source Schema

 | Property               | Example                                | Definition                                                                                      |
 | ---                    | ---                                    | ---                                                                                             |
 | `tdmq_id`              | `77bc1168-72ca-11eb-90c6-27bd7eadc75d` | Platform-assigned source id                                                                     |
 | `id`                   | `tdm/sensor_6`                         | External id                                                                                     |
 | `alias`                | "Mosaic of weather radars"             | Optional user-friendly name                                                                     |
 | `entity_category`      | "Radar"                                | One of the platform-defined entity categories                                                   |
 | `entity_type`          | MeteoRadarMosaic                       | One of the platform-defined entity types                                                        |
 | `default_footprint`    |                                        | GeoJSON-encoded Source location/area covered                                                    |
 | `stationary`           | `true`                                 | Boolean. Whether the source can move from its `default_footprint`.                              |
 | `geomapping`           |                                        |                                                                                                 |
 | `public`               | `true`                                 | Boolean. Whether the Source generates public or private data                                    |
 | `shape`                | `[ 360, 720 ]`                         | Dimensions of each element of the time series produced by the source. Empty for scalar sources. |
 | `controlledProperties` | `[ "temperature" ]`                    | List of capabilities of the Source                                                              |
 | `description`          |                                        | Metadata object of varying structure.                                                           |


### The description object

The properties found in the `description` object vary depending on the specific
entity (category, type) of the Source and, sometimes, they also vary for the
specific Source.

The following description properties *may* be defined for any Source:

| Property      | Example                         | Definition                                                                                                                                                                                               |
| ---           | ---                             | ---                                                                                                                                                                                                      |
| `brand_name`  | Acme                            | Who/what produced the Source device.                                                                                                                                                                     |
| `model_name`  | Sensor                          | Model name of the device.                                                                                                                                                                                |
| `operated_by` | Pluto                           | The entity or person responsible for the source's operation and availability                                                                                                                             |
| `reference `  | `https://pluto.com/acme_sensor` | URL to more specific information about the specific device or service.  Following this URL, you should be able to find the information required to property interpret the data generated by the device. |
| `comments`    | This is not a real sensor       | Any comment the operator or integrator


#### `description` for Scalar Sources

Generally, scalar sources consisting in sensing devices connected to an edge
will have the following description properties:
* `edge_id`
* `station_id`
* `station_model`
* `sensor_id`

For a full description see the [page dedicated to scalar
sources](scalar-metadata).


#### `description` for Non-scalar Sources

Non-scalar sources are often one-of-a-kind devices or services that are not
easily cast into a strict model.  Thus, the platform provides a minimal set of
metadata properties and a reference to documentation that provides more detailed
information about the source and the data it provides.

For a full description see the [page dedicated to non-scalar
sources](multidimensional-metadata).


## Key specific source metadata

### Field name: `default_footprint`

Georeferencing of the location or area or volume covered by the source.  If
`stationary` is true, all data from this source is from this same location;
otherwise, the time series data will include a series of `footprint` values, one
per datum.  The `default_footprint` of a source is constant and cannot vary.

The value of the `default_footprint` is a GeoJSON-encoded geography, using WGS
coordinates.

Example:
```
"default_footprint": {
    "type": "Point",
    "coordinates": [
        8.936079,
        38.990040
    ]
}
```

```
"default_footprint": {
   "coordinates": [[
       [4.537000517753033, 47.856095810774605],
       [4.537000517753033, 35.07686201381699],
       [20.436762466677894, 35.07686201381699],
       [20.436762466677894, 47.856095810774605],
       [4.537000517753033, 47.856095810774605]]],
   "type": "Polygon" },
```

### Field name: `shape`

Vector defining the dimensionality of each element of the Source's time series.
Will be empty or `null` for scalar time series.  

Example for scalar source:
```
    "shape": [],
```

Example for non-scalar source:
```
    "shape": [1400, 1200],
```

### Field name: `controlledProperties`

List of capabilities of the Source.  For each property, one can extract a time
series through the API.

:warning: `controlledProperties` defines the **capabilities** of the specific
Source.  However, the Source may not transmit data for all its theoretical
capabilities.

For [scalar sources](scalar-metadata), the specific set of
`controlledProperties` of a Source depends on its category and type.  For
[non-scalar sources](multidimensional-metadata) it depends on the specific
device:  additional information will be found in the specific Source's metadata.
