#!/usr/bin/env bash
#
# This script will build and package all of the configurations listed in the BUILD_ARGS_MATRIX array.


# Default build arguments
# To build a subset or a different set of artifacts rename the build-args-matrics.sh.example file to
# remove the .example extension and edit the array to contain the dependencies against which you'd like to build
BUILD_ARGS_MATRIX=(
  "-Daccumulo.version=1.6.0-cdh5.1.4 -Dhadoop.version=2.6.0-cdh5.4.0 -Dgeotools.version=13.0 -Dgeoserver.version=2.7.3 -Dvendor.version=cdh5 -P cloudera"
  "-Daccumulo.version=1.6.2 -Dhadoop.version=2.6.0 -Dgeotools.version=13.2 -Dgeoserver.version=2.7.3 -Dvendor.version=apache -Dvendor.version=apache -P \"\""
  "-Daccumulo.version=1.6.1.2.2.4.0-2633 -Dhadoop.version=2.6.0.2.2.4.0-2633 -Dgeotools.version=13.2 -Dgeoserver.version=2.7.3 -Dvendor.version=hdp2 -P hortonworks"
)


# Get to the right place in the file system
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/../../.."
WORKSPACE="$(pwd)"

# ngageoint is the default repo but allow for override
DOCKER_REPO=${DOCKER_REPO:-"ngageoint"}

# Remove RPMs created in previous runs
rpm-clean() {
	pushd $WORKSPACE/deploy/packaging/rpm/centos/6
    rm -rf BUILD/* BUILDROOT/* RPMS/* SRPMS/* TARBALL/*
    popd
}

# selinux config if needed
if which selinuxenabled &>/dev/null && selinuxenabled && which chcon >/dev/null ; then
	chcon -Rt svirt_sandbox_file_t $WORKSPACE;
fi

# Override build args if file is present
if [ -f $SCRIPT_DIR/build-args-matrix.sh ]; then
	source $SCRIPT_DIR/build-args-matrix.sh
fi

# Create our build command
export MVN_PACKAGE_FAT_JARS_CMD="/usr/src/geowave/deploy/packaging/rpm/admin-scripts/jenkins-build-geowave.sh $SKIP_TESTS"

# If there is no ~/.m2 directory present use a cache. (Speeds up container builds)
$WORKSPACE/deploy/packaging/docker/pull-s3-caches.sh
rpm-clean

# Build each of the configurations listed in the build matrix
for build_args in "${BUILD_ARGS_MATRIX[@]}"
do
	export BUILD_ARGS="$build_args"
	export MVN_BUILD_AND_TEST_CMD="mvn install $SKIP_TESTS $BUILD_ARGS"

	# Build the project artifacts
	docker run --rm \
		-e WORKSPACE=/usr/src/geowave \
		-e BUILD_ARGS="$build_args" \
		-e MAVEN_OPTS="-Xmx1500m" \
		-v $HOME:/root -v "${WORKSPACE}:/usr/src/geowave" \
		"${DOCKER_REPO}/geowave-centos6-java8-build" \
		/bin/bash -c \
		"cd \$WORKSPACE && $MVN_BUILD_AND_TEST_CMD && $MVN_PACKAGE_FAT_JARS_CMD"

	# RPM package everything
	docker run --rm \
		-e WORKSPACE=/usr/src/geowave \
		-e BUILD_ARGS="$build_args" \
		-v "${WORKSPACE}:/usr/src/geowave" \
		"${DOCKER_REPO}/geowave-centos6-rpm-build" \
		/bin/bash -c \
		"cd \$WORKSPACE && deploy/packaging/docker/build-rpm.sh"
done
