#!/usr/bin/env bash

export QUERY="SELECT count() from merge_tree.event_time_batch group by toYYYYMM(time)"
python selecter.py

export QUERY="SELECT count() from merge_tree.event_time_single group by toYYYYMM(time)"
python selecter.py

export QUERY="SELECT count() from merge_tree.event_time_order_func_batch group by toYYYYMM(time)"
python selecter.py

export QUERY="SELECT count() from merge_tree.event_time_order_func_single group by toYYYYMM(time)"
python selecter.py

export QUERY="SELECT count() from merge_tree.event_date_batch group by toYYYYMM(time)"
python selecter.py

export QUERY="SELECT count() from merge_tree.event_date_single group by toYYYYMM(time)"
python selecter.py

export QUERY="SELECT count() from merge_tree.event_date_batch group by toYYYYMM(date)"
python selecter.py

export QUERY="SELECT count() from merge_tree.event_date_single group by toYYYYMM(date)"
python selecter.py
