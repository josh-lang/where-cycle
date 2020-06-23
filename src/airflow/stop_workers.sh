#!/bin/bash

# Stop spark on all workers and then send stop-instances
# command to AWS

set -e

/usr/local/spark/sbin/stop-all.sh &&
aws ec2 stop-instances --instance-ids \
    $EC2_WORKER1_ID $EC2_WORKER2_ID $EC2_WORKER3_ID
