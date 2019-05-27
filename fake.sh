#!/usr/bin/env bash
# export DB_NAME=fivetuple
# export TABLE_NAME=maininfo
# export BULK_SIZE=10
# export EVENTS_PER_DAY=10
# python3 idiotpython.py
for s in `seq 1987 2018`
do
for m in `seq 1 12`
do
wget https://transtats.bts.gov/PREZIP/On_Time_Reporting_Carrier_On_Time_Performance_1987_present_${s}_${m}.zip
done
done