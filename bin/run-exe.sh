#!/usr/bin/env bash

source ./cluster.sh


tmp=$((${#localIP}-1))
machine_id=-1
firstIP="${allIPs[0]}"
remoteIPs="$firstIP"
for i in "${!allIPs[@]}"; do
	if [  "${allIPs[i]}" ==  "$localIP" ]; then
		machine_id=$i
	fi
	if [ "${allIPs[i]}" !=  "$firstIP" ]; then
        remoteIPs="${remoteIPs},${allIPs[i]}"
	fi
done


echo AllIps: "${allIPs[@]}"
echo RemoteIPs: "${remoteIPs[@]}"
echo Machine-Id "$machine_id"



export MLX5_SINGLE_THREADED=1
export MLX5_SCATTER_TO_CQE=1


sudo killall $1

# A function to echo in blue color
function blue() {
	es=`tput setaf 4`
	ee=`tput sgr0`
	echo "${es}$1${ee}"
}


#blue "Removing SHM keys used by the workers 24 -> 24 + Workers_per_machine (request regions hugepages)"
#for i in `seq 0 32`; do
#	key=`expr 24 + $i`
#	sudo ipcrm -M $key 2>/dev/null
#done

# free the  pages workers use

#blue "Removing SHM keys used by MICA"
for i in `seq 0 28`; do
	key=`expr 1185 + $i`
	sudo ipcrm -M $key 2>/dev/null
	key=`expr 2185 + $i`
	sudo ipcrm -M $key 2>/dev/null
done


#blue "Removing hugepages"
#shm-rm.sh 1>/dev/null 2>/dev/null

for i in `seq 0 64`; do		# Lossy index and circular log
	sudo ipcrm -M $i 2>/dev/null
done
sudo ipcrm -M 3185	2>/dev/null		# Request region at server
sudo ipcrm -M 3186	2>/dev/null		# Response region at server


#blue "Reset server QP registry"
sleep 1
#
#blue "Running  worker threads"
# sudo LD_LIBRARY_PATH=/usr/local/lib/ -E \
	./$1 \
	--all-ips ${remoteIPs[@]} \
	--machine-id $machine_id \
  --device_name ${NET_DEVICE_NAME} \
	2>&1
