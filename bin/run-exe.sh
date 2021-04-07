#!/usr/bin/env bash

source ./cluster.sh

#### Get CLI arguments
# Use -1 for the default (#define in config.h) values if not argument is passed
IS_REMOTE_BQR="1"
BQR_READ_BUF_LEN="0"
WRITE_RATIO="-1"
EXEC="zookeeper"

# Each letter is an option argument, if it's followed by a collum
# it requires an argument. The first colum indicates the '\?'
# help/error command when no arguments are given
while getopts ":B:R:w:x:h" opt; do
  case $opt in
     x)
       EXEC=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     w)
       WRITE_RATIO=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     B)
       BQR_READ_BUF_LEN=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     R)
       IS_REMOTE_BQR=$OPTARG # given number is divided by 10 to give write rate % (i.e., 55 means 5.5 % writes)
       ;;
     h)
      echo "Usage: -w <write ratio>  (x1000 --> 10 for 1%)"
      exit 1
      ;;
    \?)
      echo "Invalid option: -$OPTARG use -h to get info for arguments" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

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


sudo killall ${EXEC}

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
	./${EXEC} \
	--all-ips ${remoteIPs[@]} \
	--machine-id $machine_id \
  --device_name ${NET_DEVICE_NAME} \
  --write-ratio ${WRITE_RATIO} \
  --bqr-is-remote ${IS_REMOTE_BQR} \
  --bqr-buffer-size ${BQR_READ_BUF_LEN} \
	2>&1
