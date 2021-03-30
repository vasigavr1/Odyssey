#!/usr/bin/env bash



EXECUTABLE="zookeeper"

EXEC_FOLDER="${HOME}/odyssey/build"
REMOTE_COMMAND="cd ${EXEC_FOLDER}; bash ./run-exe.sh ${EXECUTABLE}"


./copy-run.sh zookeeper &
sleep 3 # give some leeway so that manager starts before executing the members

# get Hosts
source ./cluster.sh
parallel "ssh -tt {} $'${REMOTE_COMMAND} ${args}'" ::: $(echo ${REMOTE_HOSTS[@]}) >/dev/null

./get-system-xput-files.sh
