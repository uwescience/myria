#!/bin/bash
./upload_file.sh rawpointsonly
./upload_file.sh rawcomponentsonly
./convert_componentsonly.sh
./convert_pointsonly.sh
