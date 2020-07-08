#!/usr/bin/env bash

/home/s1687259/odyssey/bin/git-odyssey-push.sh "$1"
/home/s1687259/odyssey/bin/git-kite-push.sh "$1"
/home/s1687259/odyssey/bin/git-zk-push.sh "$1"
/home/s1687259/odyssey/bin/git-shared-push.sh "$1"