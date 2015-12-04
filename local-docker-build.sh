#!/bin/bash

PROJECT=${PROJECT}
echo "PROJECT is: ${PROJECT}"
BUILD_LOCATION=""
PUSH_DOCKER="NA"
ARTIFACT_NAME=""
ARTIFACT_VERSION=""
DOCKER_BUILD=docker/build

if [ -f "$DOCKER_BUILD" ]; then
    echo "Removing $DOCKER_BUILD" 
    rm -rf docker
fi

if [ -z "$PROJECT" ]; then
    echo "Please set the base path of the project."
    exit
fi

if [ -z "$1" ]; then
    echo "Arg 1 should be an Artifact name and cannot be empty."
    exit
fi

if [ -z "$2" ]; then
    echo "Arg 2 should be an version and cannot be empty."
    exit
fi

if [ -z "$3" ]; then
   echo "Push arg is empty."
   echo "A push to the internal docker repository will not take place."
fi

ARTIFACT_NAME=$1
VERSION_TAG=$2
PUSH_DOCKER=$3

export DATE_TAG=`date +%Y%m%d%H%M`
export HOSTNAME_TAG=`hostname`
#export USER=`id -F`
export USER=`id`
#export $VERSION_TAG
export DOCKER_TAG="local-${HOSTNAME_TAG}-${DATE_TAG}-${VERSION_TAG}"
JAR=".jar"

echo "DOCKER_TAG is: $DOCKER_TAG"

mkdir -p docker/build
cp ${PROJECT}/target/$ARTIFACT_NAME-$VERSION_TAG.jar docker/build/.
cp ${PROJECT}/src/test/resources/$ARTIFACT_NAME.sh docker/build/.
cd docker/build

echo "*********** Building Dockerfile **************"

cat > Dockerfile <<EOF
FROM ubuntu:14.04.2

RUN apt-get install -y software-properties-common
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y git-core curl zlib1g-dev build-essential libssl-dev libreadline-dev libyaml-dev libsqlite3-dev sqlite3 libxml2-dev libxslt1-dev libcurl4-openssl-dev python-software-properties oracle-java8-installer oracle-java8-installer vim awscli perl-base
RUN update-java-alternatives -s java-8-oracle

EXPOSE 8080

ADD ${ARTIFACT_NAME}-${VERSION_TAG}.jar /usr/local/bin/${ARTIFACT_NAME}-${VERSION_TAG}.jar
ADD ${ARTIFACT_NAME}.sh /usr/local/bin/$ARTIFACT_NAME.sh

ENV datastream.kafkaBrokers=192.168.99.100:9092
ENV datastream.schemaRegistryUrl=https://192.168.99.100:8081
ENV datastream.zookeeperConnect=192.168.99.100:2181
ENV datastream.zookeeperSyncTime=200
ENV datastream.zookeeperSyncTimeout=400
ENV datastream.consumer.metadataFetchTimeout=100
ENV datastream.consumer.groupId=testGroup
ENV datastream.topic=audience-server-bluekai
ENV datastream.producer.compressionType=snappy
ENV eventhandler.scheduler.initialDelay=1
ENV eventhandler.scheduler.period=30000
ENV webhdfs.host=dockerhadoop
ENV webhdfs.baseDir=/mydata
ENV webhdfs.fileName=myFile.txt

CMD bash -C '/usr/local/bin/webhdfs-dataloader.sh'; 'bash'

EOF

echo "*********** Dockerfile Built **************"

docker build -t docker.spongecell.net/spongecell/${ARTIFACT_NAME}:${DOCKER_TAG} .

echo "PUSH_DOCKER is $PUSH_DOCKER"

if [ "$PUSH_DOCKER" == "push" ]; then
    echo "Pushing ${ARTIFACT_NAME}:${DOCKER_TAG} to docker.spongecell.net/spongecell"
#    docker push docker.spongecell.net/spongecell/${ARTIFACT_NAME}:${DOCKER_TAG}
fi
