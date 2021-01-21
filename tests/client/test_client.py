
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
    assert all(s.id for s in sources) # when anonymizing the `id` is removed


def test_get_anonymized_source(clean_storage, db_data, source_data, live_app):
    from tdmq.db import _compute_tdmq_id
    c = Client(live_app.url())
    a_private_source = next(s for s in source_data['sources'] if not s.get('public'))
    tdmq_id = _compute_tdmq_id(a_private_source['id'])

    src = c.get_source(tdmq_id)
    assert src.id is None

    with pytest.raises(HTTPError) as exc_info:
        c.get_source(tdmq_id, anonymized=False)
    assert exc_info.value.response.status_code == 401 # unauthorized
