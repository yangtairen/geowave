#!/usr/bin/env bash
#
# This script will build a single set of rpms for a given configuration
#

# Set a default version
VENDOR_VERSION=cdh5

if [ -z $GEOSERVER_DOWNLOAD_BASE ]; then
	GEOSERVER_DOWNLOAD_BASE=https://s3.amazonaws.com/geowave/third-party-downloads/geoserver
fi

if [ -z $GEOSERVER_VERSION ]; then
	GEOSERVER_VERSION=geoserver-2.7.3-bin.zip
fi

GEOSERVER_ARTIFACT=/usr/src/geowave/deploy/packaging/rpm/centos/6/SOURCES/geoserver.zip
if [ ! -f "$GEOSERVER_ARTIFACT" ]; then
	curl $GEOSERVER_DOWNLOAD_BASE/$GEOSERVER_VERSION > $GEOSERVER_ARTIFACT
fi

if [ ! -z "$BUILD_ARGS" ]; then
	VENDOR_VERSION=$(echo "$BUILD_ARGS" | grep -oi "vendor.version=\w*" | sed "s/vendor.version=//g")
fi

echo "---------------------------------------------------------------"
echo "             Building RPM with the following settings"
echo "---------------------------------------------------------------"
echo "GEOSERVER_VERSION=${GEOSERVER_VERSION}"
echo "BUILD_ARGS=${BUILD_ARGS}"
echo "VENDOR_VERSION=${VENDOR_VERSION}"
echo "---------------------------------------------------------------"

# Staging Artifacts for Build
cd /usr/src/geowave/deploy/packaging/rpm/centos/6/SOURCES
rm -f *.gz *.jar
cp /usr/src/geowave/target/site.tar.gz .
cp /usr/src/geowave/docs/target/manpages.tar.gz .
cp /usr/src/geowave/deploy/target/*.jar .
cp /usr/src/geowave/deploy/target/*.tar.gz .
cd ..

# Oddness with Virtualbox and Docker-Machine mounting of volumes
if [ $(ls -ld /usr/src/geowave/deploy/ | awk '{print $3}') -eq 1000 ]; then
	useradd -u 1000 geowave
fi

./rpm.sh --command build --vendor-version $VENDOR_VERSION
