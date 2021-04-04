#!/usr/bin/env bash



#declare -a write_ratios=(10 20 50 100 200 500 1000)
#declare -a write_ratios=(100 200 500 1000)
declare -a write_ratios=(0)
EXECUTABLE="zookeeper"

EXEC_FOLDER="${HOME}/odyssey/build"
REMOTE_COMMAND="cd ${EXEC_FOLDER}; bash ./run-exe.sh"

# get Hosts
source ./cluster.sh

for WR in "${write_ratios[@]}"; do
  args="-x ${EXECUTABLE} -w ${WR}"
  ./copy-run.sh ${args} &
  sleep 3 # give some leeway so that manager starts before executing the members
  parallel "ssh -tt {} $'${REMOTE_COMMAND} ${args}'" ::: $(echo ${REMOTE_HOSTS[@]}) >/dev/null
  sleep 2 # give some leeway before getting into the next round
done

./get-system-xput-files.sh
