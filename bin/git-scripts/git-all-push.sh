#!/usr/bin/env bash

/home/s1687259/odyssey/bin/git-scripts/git-odyssey-push.sh "$1"
/home/s1687259/odyssey/bin/git-scripts/git-kite-push.sh "$1"
/home/s1687259/odyssey/bin/git-scripts/git-zk-push.sh "$1"
/home/s1687259/odyssey/bin/git-scripts/git-shared-push.sh "$1"