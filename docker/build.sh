#!/bin/sh
set -eu

# Compiler configuration

. submodules/beaver/lib/dlang.sh

set_dc_dver

# Install dependencies

case "$DMD" in
    dmd*   ) PKG= ;;
    1.*    ) PKG="dmd1=$DMD-$DIST" ;;
    2.*.s* ) PKG="dmd-transitional=$DMD-$DIST" ;;
    2.*    ) if [ $(echo $DMD | cut -d. -f2) -ge 077 ]; then
                PKG="dmd-compiler=$DMD dmd-tools=$DMD libphobos2-dev=$DMD"
             else
                PKG="dmd-bin=$DMD libphobos2-dev=$DMD"
             fi ;;
    *      ) echo "Unknown \$DMD ($DMD)" >&2; exit 1 ;;
esac

apt update
apt install -y --allow-downgrades \
    $PKG \
    libebtree6-dev \
    libtokyocabinet-dev \
    liblzo2-dev \
    libglib2.0-dev \
    libpcre3-dev \
    libgcrypt-dev \
    libgpg-error-dev

# Build app

make all pkg F=production
