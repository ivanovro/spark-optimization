#!/usr/bin/env bash
# Delete all envs

set -e

echo
echo ------ Cleanup VMs
echo
./cleanup_vms.sh

echo
echo ------ Deleting Disks
echo
./cleanup_disks.sh

echo
echo ------ Cleanup Firewall
echo
./cleanup_firewall.sh