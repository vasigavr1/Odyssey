#!/usr/bin/env bash


echo "----------Pushing to Kite-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-push.sh "kite" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Zookeeper-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-push.sh "zookeeper" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Od-lib-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-push.sh "odlib" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Odyssey-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-push.sh "" "$1"
echo "------------------------------------------------------"