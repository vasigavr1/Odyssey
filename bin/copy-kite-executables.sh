#!/usr/bin/env bash

#HOSTS=( "houston" "sanantonio")
#HOSTS=( "houston" "sanantonio" "philly")
#HOSTS=( "houston" "sanantonio" "austin")
#HOSTS=( "houston" "austin")
HOSTS=( "philly" "sanantonio" "austin" "indianapolis")
#HOSTS=( "austin" "houston" "sanantonio")
#HOSTS=( "austin" "sanantonio") # "houston" "sanantonio" "atlanta" "philly" )
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "philly" "baltimore" "chicago" "atlanta" "detroit")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "chicago" "atlanta" "detroit")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis" "atlanta")
#HOSTS=( "austin" "houston" "sanantonio" "indianapolis")
LOCAL_HOST=`hostname`
EXECUTABLES=("kite" "run-kite.sh")
HOME_FOLDER="/home/s1687259/kite/src/kite"
MAKE_FOLDER="/home/s1687259/kite/src/kite"
DEST_FOLDER="/home/s1687259/drf-sc-exec/src/drf-sc"

cd $MAKE_FOLDER
make clean
make
rm -rf core
cd -

for EXEC in "${EXECUTABLES[@]}"
do
	#echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
	parallel --will-cite scp ${HOME_FOLDER}/${EXEC} {}:${DEST_FOLDER}/${EXEC} ::: $(echo ${HOSTS[@]/$LOCAL_HOST})
	echo "${EXEC} copied to {${HOSTS[@]/$LOCAL_HOST}}"
done
