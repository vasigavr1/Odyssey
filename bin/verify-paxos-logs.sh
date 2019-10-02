#!/usr/bin/env bash

cd /home/s1687259/drf-sc/src/PaxosVerifier
g++ -O3 -o pv PaxosVerifier.cpp
./pv
cd -