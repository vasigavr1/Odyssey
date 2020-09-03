#!/usr/bin/env bash

#/home/s1687259/odyssey/bin/copy-kite-executables.sh "$1"
EXECUTABLE="$1"
MAKE_FOLDER="/home/s1687259/odyssey/build"
SCRIPT_FOLDER="/home/s1687259/odyssey/bin"
DEST_FOLDER="/home/s1687259/drf-sc-exec/src/drf-sc"
SCRIPT="run-exe.sh"

/home/s1687259/odyssey/bin/copy-executables.sh $EXECUTABLE $SCRIPT \
    $MAKE_FOLDER $SCRIPT_FOLDER $DEST_FOLDER

#cd  /home/s1687259/odyssey/build
#./run-exe.sh "$1"
cd $MAKE_FOLDER
$SCRIPT_FOLDER/$SCRIPT  "$EXECUTABLE"