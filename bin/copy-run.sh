#!/usr/bin/env bash

#/home/s1687259/odyssey/bin/copy-kite-executables.sh "$1"

/home/s1687259/odyssey/bin/copy-executables.sh "$1" "run-exe.sh" \
"/home/s1687259/odyssey/build" "/home/s1687259/odyssey/build" \
"/home/s1687259/drf-sc-exec/src/drf-sc"

cd  /home/s1687259/odyssey/build
./run-exe.sh "$1"