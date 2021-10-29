#!/usr/bin/env bash
# Cleanup disks for pattern firstname-lastname

gcloud compute instances list --project=spark-optimization 2>/dev/null | \
    cut -d' ' -f1 | \
    egrep -e "spark-optimization-" | \
    xargs gcloud compute instances delete --zone=europe-west1-b --quiet