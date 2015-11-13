#!/bin/bash
set -e

export DATE_TAG=`date +%Y%m%d`
export ARTIFACT_NAME="jetstream"
export DOCKER_TAG="${BRANCH##*/}-${BUILD_ID}-${DATE_TAG}"

# Create a Docker Tag
echo "export TAG=$DOCKER_TAG" | tee tag.sh

mkdir -p docker/build

mv ./target/*.jar ./docker/build/

##
## Seeking clarification on what role this script fulfills. --BC
##
### TO BE REPLACED WITH SUPERVISORD --BC
#cp ./src/test/resources/webhdfs-dataloader.sh ./docker/build/
###

cd docker
cat > Dockerfile <<EOF
FROM ubuntu:14.04.2

RUN apt-get install -y software-properties-common
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get clean
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true |\
    /usr/bin/debconf-set-selections
RUN apt-get install -y git-core curl zlib1g-dev build-essential libssl-dev \
                       libreadline-dev libyaml-dev libsqlite3-dev sqlite3 \
                       libxml2-dev libxslt1-dev libcurl4-openssl-dev \
                       python-software-properties oracle-java8-installer \
                       vim awscli perl-base supervisor
RUN update-java-alternatives -s java-8-oracle

# TODO - not sure if adding the ${ARTIFACT_NAME} is overkill here.
COPY build/webhdfs-dataloader-0.0.1-SNAPSHOT.jar /usr/local/bin/${ARTIFACT_NAME}-${DOCKER_TAG}.jar
COPY etc/supervisor/supervisord.conf /etc/supervisor/supervisord.conf
COPY supervisord/jetstream.conf /etc/supervisor/conf.d/jetstream.conf
### TO BE REPLACED WITH SUPERVISORD --BC
#COPY build/webhdfs-dataloader.sh /usr/local/bin/$ARTIFACT_NAME.sh
###

##
## Environment variables (and these in particular) in this file are
## essentially placeholders and will be overridden
## at deploy/run time. --BC
##
ENV datastream.kafkaBrokers=192.168.99.100:9092
ENV datastream.schemaRegistry=https://192.168.99.100:8081
ENV datastream.zookeeperConnect=192.168.99.100:2181
ENV webhdfs.host=dockerhadoop
ENV webhdfs.baseDir=/mydata
ENV webhdfs.fileName=myFile.txt
##

ENV datastream.zookeeperSyncTime=200
ENV datastream.zookeeperSyncTimeout=400
ENV datastream.consumer.metadataFetchTimeout=100
ENV datastream.consumer.groupId=testGroup
ENV datastream.topic=audience-server-bluekai
ENV datastream.producer.compressionType=snappy
ENV eventhandler.scheduler.initialDelay=1
ENV eventhandler.scheduler.period=30000

### Commenting this out, as the script is being replaced with supervisord process to be passed in deploy JSON. --BC
#CMD bash -C '/usr/local/bin/webhdfs-dataloader.sh'; 'bash'
###
##
## Above line rel. to clarification on prior mention of webhdfs-dataloader.sh. --BC
##

VOLUME /tmp
EXPOSE 8080

EOF
for buildfile in `ls build`; do
  echo "ADD build/${buildfile} /data/${buildfile}" >> Dockerfile
done

docker build -t docker.spongecell.net/spongecell/${ARTIFACT_NAME}:${DOCKER_TAG} .
docker push  docker.spongecell.net/spongecell/${ARTIFACT_NAME}:${DOCKER_TAG}

git tag ${DOCKER_TAG} 
git push origin ${DOCKER_TAG}


### Addenda
## Why not check the Dockerfile itself into the codebase? --BC
##
