#!/usr/bin/env bash


echo "----------Pushing to Odyssey-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-odyssey-push.sh "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Kite-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-kite-push.sh "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Zookeeper-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-zk-push.sh "$1"
echo "------------------------------------------------------"

echo "----------Pushing to Shared-------------------------"
/home/s1687259/odyssey/bin/git-scripts/git-shared-push.sh "$1"
echo "------------------------------------------------------"