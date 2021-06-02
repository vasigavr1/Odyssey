#!/usr/bin/env bash
GIT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
#cd "/home/s1687259/odyssey/$1"
cd $GIT_SCRIPT_DIR
cd "../../$1"
git add --all
git commit -m "$2"
git push
cd -