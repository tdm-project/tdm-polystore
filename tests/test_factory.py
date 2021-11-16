from tdmq.app import create_app


def test_config():
    assert not create_app({'PROMETHEUS_REGISTRY': True}).testing
    assert create_app({'TESTING': True, 'PROMETHEUS_REGISTRY': True}).testing
