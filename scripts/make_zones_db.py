#!/usr/bin/env python3


# Builds a shapefile archive of zones (the "zone db") used to anonymize the location of
# private data sources.
#
# The zone data is collected from:
# 1.  the shapefile of geographical areas of Italian municipalities [provided
# by ISTAT](https://www.istat.it/it/archivio/222527)
# 2.  the geojson file of the areas of Cagliari provided by Andrea Pinna.
#
# The generated zone db specifies coordinates in EPSG:4326.

# Requirements:
#   - geojson
#   - shapely
#   - fiona
#   - pyproj

# ComuniShapeFile = "/home/ubuntu/shapes_comuni_istat_2020-01-01/Com01012020_g_WGS84"
# QuartieriGeoJsonFile = "/home/ubuntu/quartieri_cagliari_4326.geojson"


import argparse
import logging
import sys

from typing import Any, Dict

import fiona
import fiona.crs
from fiona.collection import Collection as fiona_collection
import geojson
import pyproj

from shapely.geometry import shape as shapely_shape
from shapely.ops import transform as shapely_transform


DestinationEPSG = 4326


logger = logging.getLogger('zones')


def write_shapefile(writer: fiona_collection, path: str) -> None:
    with fiona.open(path) as shapes_input: 
        coord_transform = pyproj.Transformer.from_crs(
            pyproj.CRS(shapes_input.crs['init']),
            pyproj.CRS(DestinationEPSG),
            always_xy=True).transform
     
        def process_record(rec: Dict[str, Any]) -> Dict[str, Any]:
            return {
                'geometry': shapely_transform(coord_transform, shapely_shape(rec['geometry'])).__geo_interface__,
                'properties': {'ZONE_NAME': rec['properties']['COMUNE']}
            }

        n_records = 0
        for record in shapes_input:
            writer.write(process_record(record))
            n_records += 1
            if n_records % 1000 == 0:
                logger.info("processed %s records", n_records)

        logger.info("finished processing %s file", path)


def write_geojson(writer: fiona_collection, path: str) -> None:
    with open(path) as f:
        gj = geojson.load(f)
        for feature in gj['features']:
            record = {
                'geometry': feature['geometry'],
                'properties': {'ZONE_NAME': feature['properties']['quartiere']}
            }
            writer.write(record)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create a TDMQ zone database in shapefile format")
    parser.add_argument('destination', help="Path/basename of the output shapefile database")
    parser.add_argument('--include-shapefile', action='append', metavar='SHAPEFILE', default=[],
                        help="Specify shapefile (.shp) be included in the output database (records must contain fields 'COMUNE')")
    parser.add_argument('--include-geojson', action='append', metavar='GEOJSON', default=[],
                        help="Specify geojson to be included in the output database (properties must include key 'quartiere')")
    parser.add_argument('--log-level', choices=('DEBUG', 'INFO'), default='INFO')
    return parser


def main(args=None):
    logging.basicConfig(level=logging.INFO)
    parser = make_parser()
    opts = parser.parse_args(args)
    logger.setLevel(getattr(logging, opts.log_level))
    if not opts.include_shapefile and not opts.include_geojson:
        raise ValueError("You must specify at least one source database")

    destination_schema = {
        'geometry': 'Polygon',
        'properties': {'ZONE_NAME': 'str:50'}
    }

    logger.info("Opening zone_db destination")
    with fiona.open(opts.destination, 'w', crs=fiona.crs.from_epsg(DestinationEPSG),
                    driver='ESRI Shapefile', schema=destination_schema) as sink:

        for shpfile in opts.include_shapefile:
            logger.info("Adding shapefile %s", shpfile)
            write_shapefile(sink, shpfile)
        for geofile in opts.include_geojson:
            logger.info("Adding geojson item %s", geofile)
            write_geojson(sink, geofile)
    logger.info("Finished.  New shapefile archive is %s", opts.destination)


if __name__ == '__main__':
    main(sys.argv[1:])
