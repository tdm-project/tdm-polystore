
# Gunicorn configuration

import os
from prometheus_flask_exporter.multiprocess import GunicornPrometheusMetrics


def when_ready(_):
    metrics_port = int(os.environ.get('TDMQ_METRICS_PORT', 9100))
    GunicornPrometheusMetrics.start_http_server_when_ready(metrics_port)


def child_exit(_, worker):
    GunicornPrometheusMetrics.mark_process_dead_on_child_exit(worker.pid)
