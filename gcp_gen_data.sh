#!/bin/bash
nohup python -u push_events_to_pubsub.py --env local --topic game_events --project yonis-sandbox-20180926 --threads 1 > log.txt 2>&1 &