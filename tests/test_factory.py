from tdmq.app import create_app
from prometheus_client.registry import CollectorRegistry


def test_config():
    assert not create_app({'PROMETHEUS_REGISTRY': True}).testing
    assert create_app({'TESTING': True, 'PROMETHEUS_REGISTRY': True}).testing
