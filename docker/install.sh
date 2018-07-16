#!/bin/sh
set -xeu

# Install dependencies

apt update
apt install -y lsb-release apt-transport-https

apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 379CE192D401AB61
echo "deb https://dl.bintray.com/sociomantic-tsunami/dlang \
    $(lsb_release -cs) release prerelease" >> /etc/apt/sources.list.d/dlang.list

apt update
apt install -y libebtree6

# Prepare folder structure and install dhtnode

mkdir -p /srv/dhtnode/dhtnode-0
apt install -y /packages/dhtnode-d*.deb
ln -s /usr/sbin/dhtnode-* /usr/sbin/dhtnode
