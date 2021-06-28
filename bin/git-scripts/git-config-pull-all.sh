#!/usr/bin/env bash

#Configure git to merge when pulling

GIT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$GIT_SCRIPT_DIR/../.."

git submodule foreach --recursive git config pull.rebase false

echo "----------Configuring Odyssey-------------------------"
git config pull.rebase false
echo "------------------------------------------------------"