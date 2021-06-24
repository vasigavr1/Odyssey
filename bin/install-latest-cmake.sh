#!/usr/bin/env bash

# install latest cmake for ubuntu 18.04

# remove previous
sudo apt remove -y --purge --auto-remove cmake

wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /etc/apt/trusted.gpg.d/kitware.gpg >/dev/null

sudo apt-add-repository 'deb https://apt.kitware.com/ubuntu/ bionic main'
sudo apt update
sudo apt install -y kitware-archive-keyring
sudo rm /etc/apt/trusted.gpg.d/kitware.gpg

sudo apt update
sudo apt install -y cmake