#!/usr/bin/env bash
# Cleanup disks for pattern firstname-lastname

gcloud beta compute disks list 2>/dev/null |\
    cut -d' ' -f1 |\
    egrep -e "spark-optimization-" |\
    xargs gcloud beta compute disks delete --zone=europe-west1-b --quiet