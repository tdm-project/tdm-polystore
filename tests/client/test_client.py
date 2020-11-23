
from tdmq.client import Client


def test_strip_trailing_slash_from_url():
    some_url = 'http://web:8080/some_url'
    c = Client(some_url)
    assert c.base_url == some_url
    c = Client(some_url + '/')
    assert c.base_url == some_url


def test_connect(live_app):
    c = Client(live_app.url())
    assert not c.connected
    c.connect()
    assert c.connected
    assert c.tiledb_ctx is not None


def test_get_entity_categories_as_admin(live_app):
    c = Client(live_app.url(), auth_token=live_app.auth_token)
    r = c.get_entity_categories()
    assert 'entity_categories' in r
    assert len(r['entity_categories']) > 0
    assert { 'entity_category': 'Station' } in r['entity_categories']


def test_get_entity_categories_as_user(live_app):
    c = Client(live_app.url())
    r = c.get_entity_categories()
    assert 'entity_categories' in r
    assert len(r['entity_categories']) > 0
    assert { 'entity_category': 'Station' } in r['entity_categories']


def test_get_entity_types_as_admin(live_app):
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


def test_get_entity_types_as_user(live_app):
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
