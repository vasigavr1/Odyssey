#!/usr/bin/env bash

HOSTS=( "houston" "sanantonio")
HOSTS=( "houston" "sanantonio" "philly")
HOSTS=( "houston" "sanantonio" "austin")
#HOSTS=( "houston" "austin")
#HOSTS=( "houston" "sanantonio" "austin" "indianapolis")
#HOSTS=( "austin" "houston" "sanantonio")
HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" )
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" "baltimore" "chicago" "atlanta" "detroit")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "chicago" "atlanta" "detroit")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly")
LOCAL_HOST=`hostname`


SRC_FOLDER="/home/s1687259/drf-sc-exec/src/PaxosVerifier"
DST_FOLDER="/home/s1687259/drf-sc/src/PaxosVerifier/logs"
LOGS=("thread")

#Delete all previous logs
rm -rf ${DST_FOLDER}/${LOGS}*

#echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
parallel scp {}:${SRC_FOLDER}/${LOGS}*  ${DST_FOLDER}  ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
mv /home/s1687259/drf-sc/src/PaxosVerifier/thread* ${DST_FOLDER}

echo "${LOGS} logs copied from {${HOSTS[@]/$LOCAL_HOST}}"

cd /home/s1687259/drf-sc/src/PaxosVerifier
g++ -O3 -o pv PaxosVerifier.cpp
./pv
cd -
wc -l ${DST_FOLDER}/${LOGS}* | grep total



