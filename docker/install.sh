#!/bin/sh
set -xeu

# Install dependencies

apt update
apt install -y libebtree6

# Prepare folder structure and install dhtnode

mkdir -p /srv/dhtnode/dhtnode-0
apt install -y /packages/dhtnode-d*.deb
ln -s /usr/sbin/dhtnode-* /usr/sbin/dhtnode
