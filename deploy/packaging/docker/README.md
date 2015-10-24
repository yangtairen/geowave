## Step #1: Configure a Docker build host

A host to run the GeoWave build containers needs just Docker, Git and the Unzip commands available. Tested Docker
configurations are shown below but any OS capable of running Docker containers should work.

### Redhat7/CentOS7 Docker Build Host

```
sudo yum -y install docker git unzip
sudo groupadd docker
sudo usermod -aG docker $(whoami)
sudo su $USER
sudo systemctl start docker
sudo systemctl enable docker
```

### Ubuntu 14.04 Build Host
```
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
sudo sh -c "echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
sudo apt-get update
sudo apt-get -y install lxc-docker git unzip
```

### Mac Build Host
```
# Install Prerequisite Applications
Docker Machine and Git using brew or the standalone installers
  - brew install docker docker-machine git
  - https://docs.docker.com/installation/mac/
  - https://git-scm.com/download/mac

# Create a docker vm
docker-machine create --driver virtualbox --virtualbox-cpu-count "2" --virtualbox-memory "3072" geowave-build
eval "$(docker-machine env geowave-build)"
```

### Docker Test

Before continuing, test that Docker is available to the current user with the `docker info` command

## Step #2: Clone GeoWave Repo

From the docker build host we're going to clone the GeoWave repo and then by using volume mounts 
we'll allow the various containers to build and/or package the code without the need to then copy 
the finished artifacts back out of the container.

```
git clone --depth 1 https://github.com/ngageoint/geowave.git
```

## Step #3: Build GeoWave Artifacts and RPMs

The docker-build-rpms script will coordinate a series of container builds resulting in finished jar and rpm artifacts
built for each of the desired build configurations (ex: cdh5, hortonworks or apache).

```
export SKIP_TESTS="-Dfindbugs.skip=true -DskipFormat=true -DskipITs=true -DskipTests=true" # (Optional)
geowave/deploy/packaging/docker/docker-build-rpms.sh
```

After the docker-build-rpms.sh command has finished the rpms can be found in the 
`geowave/deploy/packaging/rpm/centos/6/RPMS/noarch/` directory adjusting the version of the OS as needed.

## Optional Step: Create Docker Images for Building

GeoWave Docker images are published to the [geoint organization](https://hub.docker.com/u/geoint/dashboard/) on Docker 
Hub but you can build them yourself if desired.

```
pushd geowave/deploy/packaging/docker
docker build -t geoint/geowave-centos6-java8-build -f geowave-centos6-java8-build.dockerfile .
docker build -t geoint/geowave-centos6-rpm-build -f geowave-centos6-rpm-build.dockerfile .
popd
```