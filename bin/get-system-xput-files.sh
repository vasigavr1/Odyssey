#!/usr/bin/env bash

# get Hosts
source ./cluster.sh


RESULT_FOLDER="${OD_HOME}/build/results/xput/per-node/"
RESULT_OUT_FOLDER_MERGE="${OD_HOME}/build/results/xput/all-nodes/"


# Gather remote files
parallel "scp {}:${RESULT_FOLDER}* ${RESULT_FOLDER} " ::: $(echo ${REMOTE_HOSTS[@]})
echo "xPut result files copied from: {${REMOTE_HOSTS}}"

# group all files
cd ${RESULT_FOLDER} > /dev/null

for file in *.txt; do echo "${file%_*.txt}"; done | sort -u | while read -r line; do
#	echo "AHAHA: $line"
	# Create an intermediate file print the 2nd line for all files with the same prefix to the same file
	awk 'FNR==1 {print $0}' ${RESULT_FOLDER}/$line* > ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
	#   Sum up the xPut of the (2nd iteration) from every node to create the final file
	awk -F ':' '{sum += $2} END {print sum}' ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt > ${RESULT_OUT_FOLDER_MERGE}/$line.txt
	rm -rf  ${RESULT_OUT_FOLDER_MERGE}/$line-inter.txt
done
cd - > /dev/null


echo "System-wide xPut results produced in ${RESULT_OUT_FOLDER_MERGE} directory!"
