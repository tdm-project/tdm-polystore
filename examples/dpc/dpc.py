# dpc specific ingestion support
import numpy as np
import tifffile as tiff
import math
import tempfile
import requests
import os

from datetime import datetime, timedelta


dpc_url = 'http://www.protezionecivile.gov.it/wide-api/wide/product/downloadProduct'

mask_value = -9999.0


dpc_mradar_mosaic_type = {
    "type": "meteoRadar",
    "name": "Mosaic_of_dpc_meteo_radars",
    "brandName": "DPC",
    "modelName": "dpc-radar-mosaic",
    "manufacturerName": "Dipartimento Protezione Civile",
    "category": ["sensor"],
    "function": ["sensing"],
    "controlledProperty": ["VMI", "SRI"],
    # FIXME no units? Are they defined by the fact that it is a meteoRadar?
    "reference": '/'.join(
        ["http://www.protezionecivile.gov.it",
         "attivita-rischi/meteo-idro/attivita/previsione-prevenzione",
         "centro-funzionale-centrale-rischio-meteo-idrogeologico",
         "monitoraggio-sorveglianza/mappa-radar"]),
}


dpc_temp_mosaic_type = {
    "type": "temperatureSensorNetwork",
    "name": "Mosaic_of_dpc_temperature_sensors",
    "brandName": "DPC",
    "modelName": "dpc-temperature-mosaic",
    "manufacturerName": "Dipartimento Protezione Civile",
    "category": ["sensor"],
    "function": ["sensing"],
    "controlledProperty": ["TEMP"],
    # FIXME no units? Celsius
    "reference": '/'.join(
        ["http://www.protezionecivile.gov.it",
         "attivita-rischi/meteo-idro/attivita/previsione-prevenzione",
         "centro-funzionale-centrale-rischio-meteo-idrogeologico",
         "monitoraggio-sorveglianza/mappa-radar"]),
}


def create_sensor_description(sensor_type_desc,
                              name, controlled_properties,
                              time_base, time_delta):
    # just to get the data shape, FIXME if it fails we are dead.
    geotiff_tags, data = gather_data(time_base,
                                     controlled_properties[0])
    T = np.array(geotiff_tags['ModelTransformation']).reshape(4, 4)
    ysize, xsize = data.shape
    # Note the nesting level:
    # http://wiki.geojson.org/GeoJSON_draft_version_6#Polygon
    coordinates = [[T.dot(v).tolist()
                    for v in
                    [[0, 0, 0, 1], [0, ysize, 0, 1], [xsize, ysize, 0, 1],
                     [xsize, 0, 0, 1], [0, 0, 0, 1]]]]
    description = {
        "name": name,
        "type": sensor_type_desc['name'],
        # FIXME not used
        "node": "dpc",
        "geometry": {"type": "Polygon", "coordinates": coordinates},
        "controlledProperty": controlled_properties,
        "timebase": time_base.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "timedelta": time_delta.total_seconds(),
        "geotiff_tags": geotiff_tags,
        "shape": list(data.shape),
        "mask_value": mask_value,
    }
    return description


def get_dpc_sensor(client, sensor_name, sensor_type_name,
                   controlled_properties, time_delta):
    sensors = client.get_sensors({'name': sensor_name})
    if len(sensors) == 0:
        # FIXME: time_base should be not earlier that 1 week ago and it
        # should align with the 1 hr boundary
        now = datetime.now()
        time_base = (datetime(now.year, now.month, now.day, now.hour) -
                     timedelta(seconds=6*24*3600))
        desc = create_sensor_description(
            client.sensor_types[sensor_type_name],
            sensor_name, controlled_properties,
            time_base, timedelta(seconds=time_delta))
        nslots = 10 * 365 * 24 * (3600 // time_delta)
        print(desc)
        client.register_sensor(desc, nslots=nslots)
        sensors = client.get_sensors({'name': sensor_name})
    assert len(sensors) == 1
    return sensors[0]


def extract_data(tif):
    page = tif.pages[0]
    data = page.asarray()
    # zeroout nan, force mask value to -9999.0
    data[np.isnan(data)] = mask_value
    return page.geotiff_tags, data


def gather_data(t, field):
    product_date = math.floor(t.timestamp() * 1000)
    payload = {'productType': field, 'productDate': product_date}
    print(dpc_url, payload)
    r = requests.post(dpc_url, json=payload)
    if r.status_code != 200:
        raise ValueError(f"Bad return code: {r.status_code}")
    # FIXME find a cleaner way to handle the returned tif file.
    handle, fpath = tempfile.mkstemp()
    with open(fpath, 'wb') as f:
        f.write(r.content)
    tif = tiff.TiffFile(fpath)
    os.unlink(fpath)
    return extract_data(tif)


def data_fetcher(sensor, t, field):
    try:
        g, d = gather_data(t, field)
    except ValueError as e:
        print(f'adding masked data for {t} {field} because of {e}.')
        d = np.zeros(sensor.get_shape())
        d.fill(mask_value)
    return d
