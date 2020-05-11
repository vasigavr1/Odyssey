#!/usr/bin/env bash

# Run the following with sudo
echo 8192 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages
echo 10000000001 > /proc/sys/kernel/shmall
echo 10000000001 > /proc/sys/kernel/shmmax
# check that it worked
cat /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages


# This is only needed if multicast is used and the interface is not defined
#ifconfig ib0 192.168.5.18  netmask 255.255.254.0

# This starts the subnet manager, run it in one machine only!
#/etc/init.d/opensmd start

#PCIe counter settings
#echo 0 > /proc/sys/kernel/nmi_watchdog
#modprobe msr


# If a machine is using huge pages of 1GB instead of 2MB
# open /etc/default/grub and write this: GRUB_CMDLINE_LINUX_DEFAULT="quiet splash hugepagesz=2M default_hugepagesz=2M"
# then run update-grub
