#!/bin/bash
set -e

export DATE_TAG=`date +%Y%m%d`

export ARTIFACT_NAME="jetstream"

export DOCKER_TAG="${BRANCH##*/}-${BUILD_ID}-${DATE_TAG}"

mkdir -p docker/build
# TODO the jar file must be pulled from S3
# s3://mvn.spongecell.com/snapshots/handler/webhdfs-dataloader/0.0.1-SNAPSHOT/webhdfs-dataloader-0.0.1-20151109.232344-16.jar
# The script webhdfs-dataloader.sh must be retrieved from some source location:
# Either s3 or from the Java source. Perhaps the jetstream build can push it to 
# s3 as well. Without this, the next two lines will fail, as the failed build indicates.
mv **/target/*.jar docker/build
cp **/src/test/resources/webhdfs-dataloader.sh docker/build/.

cd docker
cat > Dockerfile <<EOF
FROM ubuntu:14.04.2

RUN apt-get install -y software-properties-common
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true |\
    /usr/bin/debconf-set-selections
RUN apt-get install -y git-core curl zlib1g-dev build-essential libssl-dev \
                       libreadline-dev libyaml-dev libsqlite3-dev sqlite3 \
                       libxml2-dev libxslt1-dev libcurl4-openssl-dev \
                       python-software-properties oracle-java8-installer \
                       vim awscli perl-base
RUN update-java-alternatives -s java-8-oracle

EXPOSE 8080

# TODO - not sure if adding the ${ARTIFACT_NAME} is overkill here.
ADD ${ARTIFACT_NAME}-${VERSION_TAG}.jar /usr/local/bin/${ARTIFACT_NAME}-${DOCKER_TAG}.jar
ADD ${ARTIFACT_NAME}.sh /usr/local/bin/$ARTIFACT_NAME.sh

# TODO modify the ip addresses to reflect the IP Addressses
# or DNS entries for kafka, schemaRegistry, zookeeper, etc
# in Staging.
## These to be sourced from Consul -BC
ENV datastream.kafkaBrokers=192.168.99.100:9092
ENV datastream.schemaRegistry=https://192.168.99.100:8081
ENV datastream.zookeeperConnect=192.168.99.100:2181
##

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
## Why is there a trailing 'bash' on the above command?

EOF
EXPOSE 8080
VOLUME /tmp

EOF
for buildfile in `ls build`; do
  echo "ADD build/${buildfile} /data/${buildfile}" >> Dockerfile
done

docker build -t docker.spongecell.net/spongecell/${ARTIFACT_NAME}:${DOCKER_TAG} .
docker push  docker.spongecell.net/spongecell/${ARTIFACT_NAME}:${DOCKER_TAG}

git tag ${DOCKER_TAG} 
git push origin ${DOCKER_TAG}


## Addenda
# Why not check the Dockerfile itself into the codebase?
