#!/bin/bash

export DATE_TAG=`date +%Y%m%d`
export BUILD_ID=V1
export DOCKER_TAG="jenkins-${DATE_TAG}-${BUILD_ID}"

docker build -t docker.spongecell.net/spongecell/webhdfs-dataloader:${DOCKER_TAG} .
