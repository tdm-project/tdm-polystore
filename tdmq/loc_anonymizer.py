
"""
Location anonymization module.

No coordinate transformations are performed, but there is an underlying
assumption that we are working in wsg84.
"""
import logging
import os
import subprocess
import tarfile
import tempfile
import urllib.parse as up

import shapefile
import shapely
import shapely.geometry as sg
from shapely.strtree import STRtree

from tdmq.utils import find_exec

_logger = logging.getLogger(__name__)


if not shapely.speedups.enabled:
    _logger.warning("Shapely speedups are not enabled")


def _fetch_file(uri, dest):
    """
    Fetch a file from a local or remote `uri` and write it to `dest`.

    Remove uris must specify the scheme required to access them (e.g., http://...).
    Paths without a scheme are assumed to be local.
    """
    num_retries = 5
    supported_schemes = ('file', 'http', 'https', 'ftp', 'ftps')
    uri_parts = up.urlparse(uri)
    if uri_parts.scheme != '' and uri_parts.scheme not in supported_schemes:
        raise ValueError("Unsupported access protocol for LOC_ANONYMIZER_DB.  "
                         "We only support local paths and %s" + ','.join(supported_schemes))
    if os.path.exists(dest):
        raise RuntimeError(f"Download destination path {dest} already exists!")

    if uri_parts.scheme in ('', 'file'):
        src_path = uri_parts.path
        if not os.path.exists(src_path):
            raise ValueError(f"LOC_ANONYMIZER_DB local source path {src_path} does not exist!")
        os.symlink(src_path, dest)
        _logger.debug("Symlinked %s to destination path %s", src_path, dest)
    else:
        wget = find_exec('wget')
        if not wget:
            raise RuntimeError("Couldn't find wget executable in PATH")

        _logger.debug("Found wget: %s", wget)
        cmd = [ wget, '--quiet', '--timeout=5', f'--tries={num_retries}', f'--output-document={dest}', uri ]
        _logger.debug("Executing download command: %s", cmd)
        _logger.info("Downloading Zone database from %s", uri)
        subprocess.check_call(cmd)
        _logger.info("Download finished. Fetched %s bytes", os.path.getsize(dest))


def _extract_zonedb_archive(archive_path, dest):
    # Takes liberally from stack overflow answer
    # https://stackoverflow.com/questions/10060069/safely-extract-zip-or-tar-using-python
    def resolved(path):
        return os.path.realpath(os.path.abspath(path))

    def goodpath(path, base):
        # joinpath will ignore base if path is absolute
        return resolved(os.path.join(base, path)).startswith(base)

    def safe_members(members):
        base_path = resolved(dest)
        for m in members:
            if m.isfile() and goodpath(m.name, base_path):
                yield m
            else:
                _logger.warning("Skipping item in tar archive:  %s", m.name)
    with tarfile.open(archive_path) as tar:
        tar.extractall(path=dest, members=safe_members(tar.getmembers()))


def _iter_shapefile_archive(uri):
    with tempfile.TemporaryDirectory(suffix="tdmq_download") as download_dir,\
         tempfile.TemporaryDirectory(suffix="tdmq_extraction") as extraction_dir:
        dest = os.path.join(download_dir, 'archive')
        _fetch_file(uri, dest)
        _extract_zonedb_archive(dest, extraction_dir)
        shapefile_parts = []
        shapefile_extensions = ('.dbf', '.prj', '.shp', '.shx')
        for root, dirs, files in os.walk(extraction_dir):
            shapefile_parts.extend(
                os.path.join(root, os.path.splitext(f)[0])
                for f in files if os.path.splitext(f)[1] in shapefile_extensions)
        unique_set = set(shapefile_parts)
        if len(unique_set) != 1:
            _logger.error("Incompatible archive.  Expected to find exactly "
                          "one basename in shapefile archive but found %s", len(unique_set))
            _logger.error("Shape file parts found: %s", '\n'.join(unique_set))
            raise ValueError("Incompatible shapefile archive.  Expected example "
                             f"one basename but found {len(unique_set)}")

        shapefile_basename = unique_set.pop()
        _logger.info("Found shapefile %s in archive", shapefile_basename)

        with shapefile.Reader(shapefile_basename) as f:
            for shaperec in f.iterShapeRecords():
                yield dict(geometry=sg.shape(shaperec.shape),
                           properties={'name': shaperec.record['ZONE_NAME']})


class Zone:
    def __init__(self, geometry, properties):
        if not isinstance(geometry, sg.base.BaseGeometry):
            raise TypeError("Expected a shapely geometry")
        self._geometry = geometry
        self._properties = properties if properties is not None else dict()

    @property
    def geometry(self):
        return self._geometry

    @property
    def properties(self):
        return self._properties

    @property
    def centroid(self):
        if not hasattr(self, '_geometry_centroid_cache'):
            self._geometry_centroid_cache = self._geometry.centroid
        return self._geometry_centroid_cache

    @property
    def area(self):
        if not hasattr(self, '_geometry_area_cache'):
            self._geometry_area_cache = self._geometry.area
        return self._geometry_area_cache


class ZoneDB:
    def __init__(self):
        self._index_by_id = None
        self._rtree = None

    def load_zone_db(self, db_iterator):
        """
        db_reader: an iterable yielding indexable elements with keys `geometry` and `properties`.
        e.g., { 'geometry': {...}, 'properties': { ... } }
        """
        index_by_id = dict()
        for element in db_iterator:
            index_by_id[id(element['geometry'])] = Zone(element['geometry'], element['properties'])

        _logger.debug("Indexing anonymization zones with STRtree...")
        rtree = STRtree((z.geometry for z in index_by_id.values()))
        _logger.debug("finished")

        self._index_by_id = index_by_id
        self._rtree = rtree
        _logger.info("Loaded ZoneDB with %s zones", len(self._index_by_id))
        return self

    def lookup(self, shapely_geometry):
        if self._rtree is None:
            raise RuntimeError("ZoneDB not loaded")
        results = self._rtree.query(shapely_geometry)
        if len(results) == 0:
            return None
        # return the result with the smallest area
        geometry_obj = min(results, key=lambda x: x.area)
        return self._index_by_id[id(geometry_obj)]


class LocAnonymizer:
    """
    Location anonymization class.

    The current implementation maps a given coordinate to the centroid of the
    smallest containing area that is found in the configured "Zone database".
    The default zone database is the map of all Italian municipalities, plus
    the database of the "official" neighbourhoods of the city of Cagliari".

    Coordinates that are outside of the these areas will be mapped to the DefaultLocation.
    """
    # DEFAULT_LOC_ANONYMIZER_DB = "https://space.crs4.it/s/YQjJFmfdjN6RMRj/download"
    DefaultLocation = Zone(
        geometry=sg.shape({ "type": "Point", "coordinates": [ 12.492227554321289, 41.890441712228586 ] }),
        properties={'name': "Somewhere"})

    def __init__(self, app=None):
        self._zone_db = None
        self._db_source = None
        if app:
            self.init_app(app)

    @property
    def db_source(self):
        return self._db_source

    def init_app(self, flask_app):
        db_path = flask_app.config.get('LOC_ANONYMIZER_DB')
        if not db_path:
            _logger.warning("LOC_ANONYMIZER_DB not set.  All anonymized locations will point to the default location")

        if self._db_source == db_path:
            _logger.info("Using already set ZoneDB path '%s'.  Not reloading", self._db_source)
            return

        if not db_path:
            self._zone_db = None
            self._db_source = None
            _logger.info("Disabled LocAnonymizer")
        else:
            _logger.info("Initializing LocAnonymizer with database path %s", db_path)
            zone_db = ZoneDB()
            zone_db.load_zone_db(_iter_shapefile_archive(db_path))
            # operation successful
            self._zone_db = zone_db
            self._db_source = db_path

    def anonymize_location(self, geometry):
        """
        Call this function to anonymize a geometry representing a location.
        We map the geometry to the centroid of smallest intersecting zone.
        If no zones intersect, we map to the default location.
        """
        if not isinstance(geometry, sg.base.BaseGeometry):
            raise TypeError("Expected a shapely geometry")
        if self._zone_db:
            zone = self._zone_db.lookup(geometry)
            if zone:
                return zone
        return self.DefaultLocation


loc_anonymizer = LocAnonymizer()
