#!/usr/bin/env bash

rm  -rf dpc
cp  -r ../dpc .
docker build -t tdmproject/dpc_ingestor -f Dockerfile.dpc .
