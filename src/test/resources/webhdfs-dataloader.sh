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

java -jar $TARGET/webhdfs-dataloader-0.0.1-SNAPSHOT.jar > /tmp/webhdfs-dataloader.log &

echo $! > "$WEBHDFS_DATALOADER_PID"

echo "started: " $!