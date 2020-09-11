#!/usr/bin/env bash
pidof $1 | xargs -I {} sudo perf top -p {}