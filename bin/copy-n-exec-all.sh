#!/usr/bin/env bash



#declare -a write_ratios=(10 20 50 100 200 500 1000)
declare -a write_ratios=(0)
#declare -a bqr_read_buf_size=(0)
declare -a bqr_read_buf_size=(512)
declare -a remote_bqr=(0)
EXECUTABLE="zookeeper"

EXEC_FOLDER="${HOME}/odyssey/build"
REMOTE_COMMAND="cd ${EXEC_FOLDER}; bash ./run-exe.sh"

# get Hosts
source ./cluster.sh

for LR in "${remote_bqr[@]}"; do
 for WR in "${write_ratios[@]}"; do
  for BQR_LEN in "${bqr_read_buf_size[@]}"; do
    args="-x ${EXECUTABLE} -w ${WR} -B ${BQR_LEN} -R ${LR}"
    ./copy-run.sh ${args} &
    sleep 3 # give some leeway so that manager starts before executing the members
    parallel "ssh -tt {} $'${REMOTE_COMMAND} ${args}'" ::: $(echo ${REMOTE_HOSTS[@]}) >/dev/null
    sleep 3 # give some leeway before getting into the next round
  done
 done
done

./get-system-xput-files.sh
