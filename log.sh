#!/bin/bash

cd $(dirname $0)

#
docker logs -f --tail 300 pyspark
