#!/usr/bin/env python3


# Builds a shapefile archive of zones (the "zone db") used to anonymize the location of
# private data sources.
#
# The zone data is collected from:
# 1.  the shapefile of geographical areas of Italian municipalities [provided
# by ISTAT](https://www.istat.it/it/archivio/222527)
# 2.  the geojson file of the areas of Cagliari provided by Andrea Pinna.

## requirements
# geojson
# shapely
# pyshp

import argparse
import logging
import sys

import geojson
import shapefile
import shapely.geometry

ComuniShapeFile = "/home/ubuntu/shapes_comuni_istat_2020-01-01/Com01012020_g_WGS84"
QuartieriGeoJsonFile = "/home/ubuntu/quartieri_cagliari_4326.geojson"

def write_shapefile(writer, path):
    with shapefile.Reader(path) as f:
        for shaperec in f.iterShapeRecords():
            writer.shape(shaperec.shape)
            writer.record(shaperec.record['COMUNE'])


def write_geojson(writer, path):
    with open(path) as f:
        gj = geojson.load(f)
    for feature in gj['features']:
        writer.shape(feature['geometry'])
        writer.record(feature['properties']['quartiere'])


def make_parser():
    parser = argparse.ArgumentParser(description="Create a TDMQ zone database in shapefile format")
    parser.add_argument('destination', help="Path/basename of the output shapefile database")
    parser.add_argument('--include-shapefile', action='append', metavar='SHAPEFILE', default=[],
                        help="Specify shapefile be included in the output database (records must contain fields 'COMUNE')")
    parser.add_argument('--include-geojson', action='append', metavar='GEOJSON', default=[],
                        help="Specify geojson to be included in the output database (properties must include key 'quartiere')")
    return parser


def main(args=None):
    logging.basicConfig(level=logging.INFO)
    parser = make_parser()
    opts = parser.parse_args(args)
    if not opts.include_shapefile and not opts.include_geojson:
        raise ValueError("You must specify at least one source database")
    logging.info("Opening zone_db destination")
    with shapefile.Writer(opts.destination) as sink:
        sink.field('ZONE_NAME', 'C', size=35)
        for item in opts.include_shapefile:
            logging.info("Adding shapefile %s", item)
            write_shapefile(sink, item)
        for item in opts.include_geojson:
            logging.info("Adding geojson item %s", item)
            write_geojson(sink, item)
    logging.info("Finished.  New shapefile archive is %s", opts.destination)


if __name__ == '__main__':
    main(sys.argv[1:])
