#!/bin/sh

topojson \
  --id-property tract_id \
  -o topo_requests.json \
  requests.json
