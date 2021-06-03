#!/usr/bin/env bash

#Configure git to merge when pulling

GIT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$GIT_SCRIPT_DIR/../.."

echo "----------Configuring Kite-------------------------"
cd "kite"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"

echo "----------Configuring Derecho-------------------------"
cd "derecho"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"

echo "----------Configuring Hermes-------------------------"
cd "hermes"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"

echo "----------Configuring Zookeeper-------------------------"
cd "zookeeper"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"

echo "----------Configuring CHT-------------------------"
cd "cht"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"

echo "----------Configuring CRAQ-------------------------"
cd "craq"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"
#
echo "----------Configuring PAXOS-------------------------"
cd "paxos"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"

echo "----------Configuring Od-lib-------------------------"
cd "odlib"
git config pull.rebase false
cd ..
echo "------------------------------------------------------"

echo "----------Configuring Odyssey-------------------------"
git config pull.rebase false
echo "------------------------------------------------------"