#!/usr/bin/env bash

cd $1
git add --all
git commit -m "$2"
git push
cd -