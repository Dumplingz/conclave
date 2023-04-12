#!/bin/bash
tpchnum=1

for filesize in 1MB 10MB 100MB 1GB
do
    python tpch.py $filesize $tpchnum > ${filesize}_tpch_${tpchnum}_logs.txt
done