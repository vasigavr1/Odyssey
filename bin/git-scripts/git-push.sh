#!/usr/bin/env bash

cd "/home/s1687259/odyssey/$1"
git add --all
git commit -m "$2"
git push
cd -