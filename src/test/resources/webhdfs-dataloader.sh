#!/bin/bash

# For debugging only.
#set -x 

TARGET=/usr/local/bin
WEBHDFS_DATALOADER_PID=/tmp/webhdfs-dataloader-pid.txt
echo "$WEBHDFS_DATALOADER_PID"

if [ -e "$WEBHDFS_DATALOADER_PID" ]; then
    echo "Using $WEBHDFS_DATALOADER_PID"
else
    echo "Creating" $WEBHDFS_DATALOADER_PID
    touch $WEBHDFS_DATALOADER_PID
    chmod 755 $WEBHDFS_DATALOADER_PID
fi

if [ "$1" = "start" ] ; then
     java -jar -Ddatastream.kafkaBrokers=192.168.99.100:9092 \
        -Ddatastream.schemaRegistry=https://192.168.99.100:8081  \
        -Ddatastream.zookeeperConnect=192.168.99.100:2181 \
        -Ddatastream.zookeeperSyncTime=200  \
        -Ddatastream.zookeeperSyncTimeout=400 \
        -Ddatastream.consumer.metadataFetchTimeout=100  \
        -Ddatastream.consumer.groupId=testGroup \
        -Ddatastream.topic=audience-server-bluekai \
        -Ddatastream.producer.compressionType=snappy  \
        -Deventhandler.scheduler.initialDelay=1 \
        -Deventhandler.scheduler.period=30000  \
        -Dwebhdfs.host=dockerhadoop \
        -Dwebhdfs.baseDir=/mydata  \
        -Dwebhdfs.fileName=myFile.txt  \
        $TARGET/webhdfs-dataloader-0.0.1-SNAPSHOT.jar > /tmp/webhdfs-dataloader.log &

        echo $! > "$WEBHDFS_DATALOADER_PID"

        echo "started: " $!
        
elif [ "$1" = "stop" ] ; then
     echo "Using PID file: $WEBHDFS_DATALOADER_PID"
     PID=`cat $WEBHDFS_DATALOADER_PID"`
     echo "Stopping $PID"
     kill -15 $PID
     if [ $? -gt 0 ]; then
          echo "PID file found but no matching process was found. Stop aborted."
          exit 1
     fi
                                                                                                                                                    
     echo "stopped: $PID"                                                                                                                           
fi
