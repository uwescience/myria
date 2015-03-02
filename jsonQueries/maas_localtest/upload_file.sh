#!/bin/bash
myria_upload --hostname localhost --port 8753 --no-ssl --overwrite --relation $1 $1.csv
