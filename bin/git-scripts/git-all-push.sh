#!/usr/bin/env bash

GIT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$GIT_SCRIPT_DIR/../.."
git submodule foreach --recursive git add --all
git submodule foreach --recursive git commit -m "$1"
git submodule foreach --recursive git push