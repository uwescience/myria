#!/bin/bash
# Get command line param, the number of EM steps
if [ "$#" -eq "1" ]
then
    N=$1
else
    N=1
fi
python ./GMM_Python_Comparison.py $N
./upload_testdata.sh
./full_upload.sh
# Do the EM step the number of times passed in on the command line
for i in `seq 1 $N`;
do
    ./full_EMstep.sh
    sleep 1
done    
./test_compare.sh
