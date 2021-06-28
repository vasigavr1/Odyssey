#!/usr/bin/env bash

GIT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
echo "----------Pushing to Kite-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "kite" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Derecho-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "derecho" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Hermes-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "hermes" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Zookeeper-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "zookeeper" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to CHT-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "cht" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to CRAQ-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "craq" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to PAXOS-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "paxos" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Od-lib-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "odlib" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Odyssey-------------------------"
$GIT_SCRIPT_DIR/git-push.sh "" "$1"
echo "------------------------------------------------------"