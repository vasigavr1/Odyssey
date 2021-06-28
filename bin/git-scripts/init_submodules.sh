#!/usr/bin/env bash

GIT_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$GIT_SCRIPT_DIR/../.."
git submodule update --init
git submodule foreach --recursive git checkout master