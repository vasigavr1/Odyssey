#!/usr/bin/env bash


echo "----------Pushing to Kite-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-kite-push.sh "kite" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Zookeeper-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-zk-push.sh "zookeeper" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Od-lib-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-odlib-push.sh "odlib" "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Odyssey-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-odyssey-push.sh "" "$1"
echo "------------------------------------------------------"