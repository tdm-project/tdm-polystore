FROM python:3.7

RUN apt-get -y update && apt-get -y install --no-install-recommends \
      libspatialindex-c4v5 \
    && apt-get clean && rm -rf /var/lib/apt-lists/* /tmp/* /var/tmp/* \
    && pip install --no-cache-dir --upgrade \
      numpy \
      osmnx
