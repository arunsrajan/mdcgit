#!/bin/sh
echo $CORE_CONF_fs_defaultFS
echo $HDFS_CONF_dfs_datanode_data_dir
cd "/opt/mdc/bin"
./container.sh &
cd "/"
./entrypoint.sh
./run.sh