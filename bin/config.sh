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


### PROFILE GUIDED OPTIMIZATIONS
# Use -fprofile-generate=./profile_info, both as a compile and link flag
# To use the profile information add -fprofile-use=./profile_info
# The errors when using are because counters used in profile generation are not thread-safe
# The flag -fprofile-correction uses heuristics to "correct" the counters
# If the code has changed slightly since profiling use -Wno-coverage-mismatch, to avoid the errors


### PROFILING GUIDELINES
# Using "perf top" (no hyphen) gives real-time information on where time goes
# To make it work we need: -g -fno-inline -fno-omit-frame-pointer
# For zoom, we must execute "sudo zoom run --allow_power_management" and then start profiling while kite executes
#pidof hermes | xargs -I {} sudo perf top -p {}