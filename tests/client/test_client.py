
import pytest
from requests.exceptions import HTTPError
from tdmq.client import Client


def test_strip_trailing_slash_from_url():
    some_url = 'http://web:8080/some_url'
    c = Client(some_url)
    assert c.base_url == some_url
    c = Client(some_url + '/')
    assert c.base_url == some_url


def test_connect(clean_storage, live_app):
    c = Client(live_app.url())
    assert not c.connected
    c.connect()
    assert c.connected
    assert c.tiledb_ctx is not None


def test_get_entity_categories_as_admin(clean_storage, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    r = c.get_entity_categories()
    assert 'entity_categories' in r
    assert len(r['entity_categories']) > 0
    assert { 'entity_category': 'Station' } in r['entity_categories']


def test_get_entity_categories_as_user(clean_storage, live_app):
    c = Client(live_app.url())
    r = c.get_entity_categories()
    assert 'entity_categories' in r
    assert len(r['entity_categories']) > 0
    assert { 'entity_category': 'Station' } in r['entity_categories']


def test_get_entity_types_as_admin(clean_storage, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    r = c.get_entity_types()
    assert 'entity_types' in r
    the_list = r['entity_types']
    assert len(the_list) > 0

    first_item = the_list[0]
    assert 'entity_category' in first_item
    assert 'entity_type' in first_item
    assert 'schema' in first_item

    next( x for x in the_list if x['entity_category'] == 'Station' and x['entity_type'] == 'WeatherObserver' )
    # if the item is missing we should get a StopIteration exception


def test_get_entity_types_as_user(clean_storage, live_app):
    c = Client(live_app.url())
    r = c.get_entity_types()
    assert 'entity_types' in r
    the_list = r['entity_types']
    assert len(the_list) > 0

    first_item = the_list[0]
    assert 'entity_category' in first_item
    assert 'entity_type' in first_item
    assert 'schema' in first_item

    next( x for x in the_list if x['entity_category'] == 'Station' and x['entity_type'] == 'WeatherObserver' )
    # if the item is missing we should get a StopIteration exception


def test_find_source_only_public(clean_storage, db_data, source_data, live_app):
    c = Client(live_app.url())
    sources = c.find_sources()
    n_public_sources = len(sources)
    assert all(s.public for s in sources)
    sources = c.find_sources(args={'only_public': False})
    assert any(not s.public for s in sources)
    assert len(sources) > n_public_sources


def test_find_source_not_anonymized(clean_storage, db_data, source_data, live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    sources = c.find_sources(args={'public': False, 'anonymized': False})
    assert all(not s.public for s in sources)
    assert all(s.id for s in sources)  # when anonymizing the `id` is removed


def test_find_source_private_find_by_id(clean_storage, db_data, source_data, live_app):
    from tdmq.db import _compute_tdmq_id
    c = Client(live_app.url())
    private_src_id = 'tdm/sensor_7'
    sources = c.find_sources(args={'id': private_src_id})
    assert len(sources) == 0

    sources = c.find_sources(args={'id': private_src_id, 'only_public': False})
    assert len(sources) == 1
    tdmq_id = str(_compute_tdmq_id(private_src_id))
    assert sources[0].tdmq_id == tdmq_id


def test_extra_properties(clean_storage, db_data, live_app):
    c = Client(live_app.url())
    src_id = 'tdm/tiledb_sensor_6'
    src = c.find_sources(args={'id': src_id})[0]

    expected_attributes = (
        "reference",
        "brand_name",
        "model_name",
        "operated_by")
    assert all(getattr(src, s) for s in expected_attributes)
    assert src.comments is None


def test_get_anonymized_source(clean_storage, db_data, source_data, live_app):
    from tdmq.db import _compute_tdmq_id
    c = Client(live_app.url())
    a_private_source = next(s for s in source_data['sources'] if not s.get('public'))
    tdmq_id = _compute_tdmq_id(a_private_source['id'])

    src = c.get_source(tdmq_id)
    assert src.alias is None

    with pytest.raises(HTTPError) as exc_info:
        c.get_source(tdmq_id, anonymized=False)
    assert exc_info.value.response.status_code == 401  # unauthorized


def test_imports():
    from tdmq.client import Source  # noqa: F401
    from tdmq.client import TimeSeries  # noqa: F401
