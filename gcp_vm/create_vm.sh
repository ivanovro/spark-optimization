#!/usr/bin/env bash
# Create VM with boot from existing disk

vm=$1

gcloud compute instances create $vm --project=spark-optimization --zone=europe-west1-b \
  --machine-type=e2-standard-8 --network-interface=network-tier=PREMIUM,subnet=default \
  --maintenance-policy=MIGRATE --service-account=38762434749-compute@developer.gserviceaccount.com \
  --scopes=https://www.googleapis.com/auth/devstorage.read_only,https://www.googleapis.com/auth/logging.write,https://www.googleapis.com/auth/monitoring.write,https://www.googleapis.com/auth/servicecontrol,https://www.googleapis.com/auth/service.management.readonly,https://www.googleapis.com/auth/trace.append \
  --disk=boot=yes,device-name=$vm,mode=rw,name=$vm --no-shielded-secure-boot --shielded-vtpm \
  --shielded-integrity-monitoring --reservation-affinity=any