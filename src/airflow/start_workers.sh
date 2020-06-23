#!/bin/bash

# Send start command to spark_worker instances, wait for all three
# to reach 'running' state, sleep for 15 more seconds just to be
# safe, and then launch spark

set -e

aws ec2 start-instances --instance-ids \
    $EC2_WORKER1_ID $EC2_WORKER2_ID $EC2_WORKER3_ID &&
aws ec2 wait instance-running --instance-ids \
    $EC2_WORKER1_ID $EC2_WORKER2_ID $EC2_WORKER3_ID &&
sleep 15 &&
/usr/local/spark/sbin/start-all.sh
