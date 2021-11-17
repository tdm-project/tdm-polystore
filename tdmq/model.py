

from __future__ import annotations

import logging
from typing import Any, Dict, Generator, Iterable, List

import pyproj
import shapely.geometry as sg
from shapely.ops import transform as shapely_transform

import tdmq.db as db
from .loc_anonymizer import loc_anonymizer

logger = logging.getLogger(__name__)


class EntityType:
    @staticmethod
    def get_entity_types() -> List[str]:
        return db.list_entity_types()


class EntityCategory:
    @staticmethod
    def get_entity_categories() -> List[str]:
        return db.list_entity_categories()


class Source:
    ROI_CENTER_DIGITS = 3
    ROI_RADIUS_INCREMENT = 500

    # Property required by Source structure, as specified in the API spec.
    RequiredKeys = frozenset({
        'default_footprint',
        'entity_category',
        'entity_type',
        'external_id',
        'description',
        'public',
        'registration_time',
        'stationary',
        'tdmq_id',
        })

    # Keys in Source structure that do not require anonymization
    SafeKeys = frozenset({
        'entity_category',
        'entity_type',
        'public',
        'registration_time',
        'stationary',
        'tdmq_id'})

    # Keys in Source.description structure that do not require anonymization
    SafeDescriptionKeys = frozenset({
        'brand_name',
        'controlledProperties',
        'edge_id',
        'model_name',
        'operated_by',
        'sensor_id',
        'shape',
        'station_id',
        'station_model',
        'type',
        })

    @staticmethod
    def store_new(data: Iterable[dict]) -> List[str]:
        return db.load_sources(data)

    @classmethod
    def _in_place_form_api_source(cls, db_source: dict) -> None:
        # Take the `description.description` and move its contents to the top level dict
        db_source['description'].update(db_source['description'].pop('description'))

    @classmethod
    def get_one(cls, tdmq_id: str, anonymize_private: bool = True) -> dict:
        srcs = db.get_sources([tdmq_id])
        if not srcs:
            return None

        if len(srcs) == 1:
            source = srcs[0]
            cls._in_place_form_api_source(source)
            if anonymize_private and not source.get('public'):
                source = cls._anonymize_source(source)
            return source
        # Somehow we got more than one results from the query
        raise RuntimeError(f"Got more than one source for tdmq_id {tdmq_id}")

    @classmethod
    def get_latest_activity(cls, tdmq_id: str) -> Dict[str, Any]:
        # ensure the tdmq_id is valid
        srcs = db.get_sources([tdmq_id])
        if not srcs:
            return None

        retval = dict.fromkeys(('tdmq_id', 'time', 'data'))
        retval['tdmq_id'] = tdmq_id

        activity = db.get_latest_activity(tdmq_id)
        # activity is None or a dict { 'time': timestamp, 'data': [ record data objects ] }
        if activity is not None:
            retval['time'] = activity['time']
            if len(activity['data']) == 1:
                retval['data'] = activity['data'][0]
            elif len(activity['data']) > 1:
                logger.debug("get_latest_activity: merging %s data rows from source %s with timestamp %s",
                             len(activity['data']), tdmq_id, activity['time'])
                retval['data'] = dict()
                for d in activity['data']:
                    retval['data'].update(d)
            else:
                raise RuntimeError(f"Internal error.  For source {tdmq_id} got latest "
                                   "activity time {activity['time']} with no activity data")
        return retval

    @staticmethod
    def delete_one(tdmq_id: str) -> None:
        db.delete_sources([tdmq_id])

    # AcceptedSearchKeys: set of search parameters accepted for source search.
    AcceptedSearchKeys = frozenset({
        'after',
        'before',
        'entity_category',
        'entity_type',
        'external_id',
        'id',
        'public',
        'roi',
        'stationary',
        })

    @classmethod
    def _anonymize_source(cls, src_dict: dict) -> dict:
        sanitized_desc = dict()
        for k in cls.SafeDescriptionKeys:
            if k in src_dict['description']:
                sanitized_desc[k] = src_dict['description'][k]

        sanitized = dict.fromkeys(cls.RequiredKeys)
        sanitized['description'] = sanitized_desc
        for k in cls.SafeKeys:
            if k in src_dict:
                sanitized[k] = src_dict[k]

        geo = sg.shape(src_dict['default_footprint'])
        anon_zone = loc_anonymizer.anonymize_location(geo)
        sanitized['default_footprint'] = anon_zone.centroid.__geo_interface__

        return sanitized

    @classmethod
    def _anonymizing_iter(cls, sources: Iterable[dict]) -> Generator[dict, None, None]:
        for s in sources:
            yield cls._anonymize_source(s)

    @classmethod
    def _quantize_roi(cls, roi: dict) -> None:
        # Round the coordinates and radius of the ROI to limit precision
        roi['center']['coordinates'] = [ round(c, cls.ROI_CENTER_DIGITS) for c in roi['center']['coordinates'] ]
        roi['radius'] = cls.ROI_RADIUS_INCREMENT * round(roi['radius'] / cls.ROI_RADIUS_INCREMENT)

    @classmethod
    def _roi_intersection_filter(cls, roi: dict, sources: Iterable[dict]) -> Generator[dict, None, None]:
        # filter sources that end up outside ROI because of anonymization
        # Geom specify wgs84 coordinates.
        wgs84 = pyproj.CRS('EPSG:4326')
        mm = pyproj.CRS('EPSG:3003')  # Monte Mario
        mm_projection = pyproj.Transformer.from_crs(wgs84, mm, always_xy=True).transform

        # project both ROI and geometry to Monte Mario coordinates
        # then we can use shapely's distance functions
        mm_roi_center = shapely_transform(mm_projection, sg.Point(roi['center']['coordinates']))
        mm_roi = mm_roi_center.buffer(roi['radius'])

        for s in sources:
            mm_geom = shapely_transform(mm_projection, sg.shape(s['default_footprint']))
            if mm_geom.intersects(mm_roi):
                yield s

    @classmethod
    def search(cls, search_args: Dict[str, Any], match_attr: Dict[str, Any] = None, anonymize_private: bool = True,
               limit: int = None, offset: int = None) -> list:
        """
        search_args: any from AcceptedSearchKeys
        match_attr:  general attribute matching in the
        Source.description.description structure.
        """
        if limit or offset:
            raise NotImplementedError("Limit and offset are not implemented")

        query_args = search_args.copy()  # copy so we can modify the dictionary

        e_id = query_args.pop('external_id', None)
        if e_id:
            query_args['id'] = e_id

        public = query_args.get('public', None)
        # unless the request is exclusively for public sources, tweak the ROI to limit precision
        if not public and 'roi' in query_args:
            cls._quantize_roi(query_args['roi'])

        if match_attr:
            query_args.update(match_attr)

        # Generally, queries that container "unsafe" search keys will be limited to
        # private sources.  However, we allow querying private sources by specific
        # external source id.
        if not ((cls.SafeKeys | {'id', 'external_id'}) >= query_args.keys() and
                cls.SafeDescriptionKeys >= match_attr.keys()):
            # can't do query on private sources because it uses unsafe attributes
            query_args['public'] = True

        raw = db.list_sources(query_args)
        for source in raw:
            cls._in_place_form_api_source(source)

        resultset = [ r for r in raw if r['public'] ]
        private_it = (r for r in raw if not r['public'])
        if anonymize_private:
            private_it = cls._anonymizing_iter(private_it)
        if 'roi' in query_args:
            private_it = cls._roi_intersection_filter(query_args['roi'], private_it)
        resultset.extend(private_it)

        return resultset


class Timeseries:
    @staticmethod
    def store_new_records(data: Iterable[dict]) -> int:
        return db.load_records(data)

    @staticmethod
    def _restructure_timeseries(rows: List[list], properties: List[str]) -> Dict[str, Any]:
        # The arrays time and footprint define the scaffolding on which
        # the actual data (properties) are defined.
        result = {'coords': None, 'data': None}
        transpose = zip(*rows) if len(rows) > 0 else iter([[]] * (2 + len(properties)))
        result['coords'] = dict((p, next(transpose)) for p in ['time', 'footprint'])
        result['data'] = dict((p, next(transpose)) for p in properties)

        # remove the arrays that are all None values
        if len(result['coords']['time']) > 0:
            if all(x is None for x in result['coords']['footprint']):
                result['coords']['footprint'] = None
            for k in [ k for k in result['data'].keys() ]:  # materialize it since we need to change the dict
                if all(x is None for x in result['data'][k]):
                    result['data'][k] = None

        return result

    @classmethod
    def get_one(cls, tdmq_id: str, anonymize_private: bool = True, args: Dict[str, Any] = None) -> Dict[str, Any]:
        if not args:
            args = dict()

        db_result = db.get_timeseries(tdmq_id, args)

        struct = cls._restructure_timeseries(db_result['rows'], db_result['properties'])
        struct["tdmq_id"] = tdmq_id
        struct["shape"] = db_result['source_info']['shape']
        if args['bucket']:
            struct["bucket"] = {
                "interval": args['bucket'].total_seconds(), "op": args.get("op")}
        else:
            struct['bucket'] = None

        # If private data is not to be returned, we erase the mobile footprint
        # from the result by replacing it with nulls.  Otherwise, we leave
        # location data in the result: i.e., # the default_footprint and the
        # timestamped footprint.
        if anonymize_private and not db_result.get('public'):
            # pylint: disable=unsubscriptable-object,unsupported-assignment-operation
            struct["coords"]["footprint"] = None
        else:
            struct["default_footprint"] = db_result['source_info']['default_footprint']

        return struct

    class QueryResult:
        def __init__(self,
                     tdmq_id: str, shape: Dict, bucket: Dict,
                     default_footprint: Dict,
                     db_query_result, anonymize_private: bool = True):
            self._tdmq_id = tdmq_id
            self._anonymize_private = anonymize_private
            self._shape = shape
            self._bucket = bucket
            self._default_footprint = default_footprint
            self._db_query_result = db_query_result

        @property
        def tdmq_id(self):
            return self._tdmq_id

        @property
        def anonymize_private(self):
            return self._anonymize_private

        @property
        def shape(self):
            return self._shape

        @property
        def bucket(self):
            return self._bucket

        @property
        def default_footprint(self):
            return self._default_footprint

        @property
        def fields(self):
            return self._db_query_result.fields

        def __iter__(self):
            return self

        def __next__(self):
            row_batch = next(self._db_query_result)

            # If private data is not to be returned, we erase the mobile footprint
            # from the result by replacing it with nulls.  Otherwise, we leave
            # location data in the result
            if self._anonymize_private and not self._db_query_result.is_public:
                for row in row_batch:
                    row[1] = None
            return row_batch

    @classmethod
    def get_one_by_batch(cls, tdmq_id: str, anonymize_private: bool = True,
                         batch_size: int = None, args: Dict[str, Any] = None) -> Generator[Dict[str, Any]]:
        if not args:
            args = dict()

        ts_result = db.get_timeseries_result(tdmq_id, batch_size, **args)

        if args['bucket']:
            bucket = {
                "interval": args['bucket'].total_seconds(), "op": args.get("op")}
        else:
            bucket = None

        # If private data is not to be returned, we don't provide the footprint
        if not anonymize_private or ts_result.is_public:
            default_footprint = ts_result.source_info['default_footprint']
        else:
            default_footprint = None

        result = cls.QueryResult(tdmq_id=tdmq_id,
                                 shape=ts_result.source_info['shape'],
                                 bucket=bucket,
                                 default_footprint=default_footprint,
                                 db_query_result=ts_result,
                                 anonymize_private=anonymize_private)
        return result
