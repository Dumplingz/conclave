#!/bin/bash
tpchnum=6

for filesize in 10MB 100MB 1GB
do
    python tpch.py $filesize $tpchnum > ${filesize}_tpch_${tpchnum}_logs.txt
done
