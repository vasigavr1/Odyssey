#!/usr/bin/env bash

source ./cluster.sh
#/home/s1687259/odyssey/bin/copy-kite-executables.sh "$1"
EXECUTABLE="$1"
MAKE_FOLDER="${OD_HOME}/build"
SCRIPT_FOLDER="${OD_HOME}/bin"
DEST_FOLDER=$MAKE_FOLDER
SCRIPT="run-exe.sh"

${SCRIPT_FOLDER}/copy-executables.sh $EXECUTABLE $SCRIPT \
    $MAKE_FOLDER $SCRIPT_FOLDER $DEST_FOLDER

#cd  /home/s1687259/odyssey/build
#./run-exe.sh "$1"
cd $MAKE_FOLDER
sleep 2
$SCRIPT_FOLDER/$SCRIPT  "$EXECUTABLE"