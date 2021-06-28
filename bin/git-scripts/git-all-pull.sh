#!/usr/bin/env bash

GIT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$GIT_SCRIPT_DIR/../.."

echo "----------Pulling from Kite-------------------------"
cd "kite" 
git pull origin master
cd ..
echo "------------------------------------------------------"

echo "----------Pulling from Derecho-------------------------"
cd "derecho" 
git pull origin master
cd ..
echo "------------------------------------------------------"

echo "----------Pulling from Hermes-------------------------"
cd "hermes" 
git pull origin master
cd ..
echo "------------------------------------------------------"

echo "----------Pulling from Zookeeper-------------------------"
cd "zookeeper" 
git pull origin master
cd ..
echo "------------------------------------------------------"

echo "----------Pulling from CHT-------------------------"
cd "cht" 
git pull origin master
cd ..
echo "------------------------------------------------------"

echo "----------Pulling from CRAQ-------------------------"
cd "craq" 
git pull origin master
cd ..
echo "------------------------------------------------------"
#
echo "----------Pulling from PAXOS-------------------------"
cd "paxos" 
git pull origin master
cd ..
echo "------------------------------------------------------"

echo "----------Pulling from Od-lib-------------------------"
cd "odlib" 
git pull origin master
cd ..
echo "------------------------------------------------------"

echo "----------Pulling from Odyssey-------------------------"
git pull origin master
echo "------------------------------------------------------"