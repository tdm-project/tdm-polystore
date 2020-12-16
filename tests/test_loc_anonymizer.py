
import os
import tempfile

import pytest
import shapely.geometry as sg

import tdmq.loc_anonymizer as la


def test_fetch_local_file(local_zone_db):
    with tempfile.TemporaryDirectory() as d_name:
        dest_name = os.path.join(d_name, "the_file")
        # pylint: disable=protected-access
        la._fetch_file(local_zone_db, dest_name)
        assert os.path.exists(dest_name)
        assert os.access(dest_name, os.R_OK)


def test_extract_archive(local_zone_db):
    with tempfile.TemporaryDirectory() as d_name:
        # pylint: disable=protected-access
        la._extract_zonedb_archive(local_zone_db, d_name)
        files = [ f for f in os.listdir(d_name) if f not in ('.', '..') ]
        assert files
        assert all(f.startswith('test_zone_db.') for f in files)
        shapefile_extensions = ('.dbf', '.shp', '.shx', '.prj')
        assert all(os.path.splitext(f)[1] in shapefile_extensions for f in files)


def test_zone_basics(a_geojson_feature):
    zone = la.Zone(sg.shape(a_geojson_feature.geometry), a_geojson_feature.properties)
    assert id(zone.centroid) == id(zone.centroid) # these should be cached
    assert id(zone.area) == id(zone.area) # these should be cached
    assert pytest.approx(8.9357, zone.centroid.x)
    assert pytest.approx(38.9900, zone.centroid.y)
    assert zone.area > 0


def test_loc_anonimizer_init(local_zone_db, app):
    app.config['LOC_ANONYMIZER_DB'] = local_zone_db
    anonymizer = la.LocAnonymizer(app)
    assert anonymizer.db_source == local_zone_db


def test_loc_anonimizer_anonymize_location(local_zone_db, app):
    app.config['LOC_ANONYMIZER_DB'] = local_zone_db
    anonymizer = la.LocAnonymizer(app)

    # The `local_zone_db` fixture only contains the neighbourhoods of the
    # city of Cagliari.
    somewhere_in_capoterra = sg.Point(8.991029, 39.159422)
    anon_zone = anonymizer.anonymize_location(somewhere_in_capoterra)
    assert anon_zone is anonymizer.DefaultLocation

    via_roma = sg.Point(9.111872, 39.214212)
    anon_zone = anonymizer.anonymize_location(via_roma)
    assert anon_zone.properties['name'] == "MARINA"
