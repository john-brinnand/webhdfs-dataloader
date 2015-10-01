FROM ubuntu:14.04.2

RUN apt-get install -y software-properties-common
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys EEA14886
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y git-core curl zlib1g-dev build-essential libssl-dev libreadline-dev libyaml-dev libsqlite3-dev sqlite3 libxml2-dev libxslt1-dev libcurl4-openssl-dev python-software-properties oracle-java8-installer oracle-java8-installer vim awscli perl-base
RUN update-java-alternatives -s java-8-oracle

ADD target/webhdfs-dataloader-0.0.1-SNAPSHOT.jar /usr/local/bin/webhdfs-dataloader-0.0.1-SNAPSHOT.jar
ADD src/test/resources/webhdfs-dataloader.sh /usr/local/bin/webhdfs-dataloader.sh

EXPOSE 8080
