
import prometheus_client

from tdmq.app import create_app


def test_config():
    assert not create_app(prom_registry=prometheus_client.CollectorRegistry()).testing
    assert create_app({'TESTING': True}, prom_registry=prometheus_client.CollectorRegistry()).testing
