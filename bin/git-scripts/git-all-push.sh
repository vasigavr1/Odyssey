#!/usr/bin/env bash


echo "----------Pushing to Kite-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-kite-push.sh "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Zookeeper-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-zk-push.sh "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Od-lib-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-odlib-push.sh "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Odyssey-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-odyssey-push.sh "$1"
echo "------------------------------------------------------"