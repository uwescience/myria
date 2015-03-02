#!/bin/bash
./upload_file.sh raw_expected_points
./upload_file.sh raw_expected_components
./convert_testcomponents.sh
./convert_testpoints.sh
