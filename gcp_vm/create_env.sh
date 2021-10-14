#!/usr/bin/env bash
# Install VM and configure firewall

set -e

vm=$1
vm="${vm//./-}"
uip=$2

echo
echo ------ Create Disk $vm
echo
./create_disk.sh $vm

echo
echo ------ Create VM $vm
echo
./create_vm.sh $vm

echo
echo ------ Update Firewall
echo
./update_firewall.sh $vm $uip