The MDC can be build using the following maven goals

clean jar:test-jar package assembly:assembly resources:copy-resources@copy-resources-dockermdccontainer resources:copy-resources@copy-resources-dockermdctss resources:copy-resources@copy-resources-dockermdcts

In order to skip tests the following needs to be set in MAVEN_OPTS

-Dmaven.test.skip.exec=true

In order to build docker images please execute the following maven goals
------------------------------------------------------------------------
-Pmdccontainer docker:build

-Pmdctaskschedulerstream docker:build

-Pmdctaskscheduler docker:build



In order to build docker images using docker commands
-----------------------------------------------------
docker build -t arunsrajan/mdccontainer .
docker push arunsrajan/mdccontainer
docker build -t arunsrajan/mdctaskschedulerstream .
docker push arunsrajan/mdctaskschedulerstream
docker build -t arunsrajan/mdctaskscheduler .
docker push arunsrajan/mdctaskscheduler.


To execute all tests only Stream Modulese
---------------------------------------
clean install -Dtest=Massive*,Pipeline* -pl MassiveDataCommon,MassiveDataStream test -DfailIfNoTests=false


To run docker container using bridge network use the following command
-----------------------------------------------------------------------

To run as 3 node zookeeper ensemble in separate bridge network
--------------------------------------------------------------
docker network create zoocluster -d bridge

docker run -d -p 2181:2181 -e ZOO_MY_ID=1 -e ZOO_SERVERS="server.1=0.0.0.0:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=zoo3:2888:3888;2181" --network=zoocluster --hostname=zoo1 -d --name=zookeeper1 zookeeper

docker run -d -p 2182:2181 -e ZOO_MY_ID=2 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=0.0.0.0:2888:3888;2181 server.3=zoo3:2888:3888;2181" --network=zoocluster --hostname=zoo2 -d --name=zookeeper2 zookeeper

docker run -d -p 2183:2181 -e ZOO_MY_ID=3 -e ZOO_SERVERS="server.1=zoo1:2888:3888;2181 server.2=zoo2:2888:3888;2181 server.3=0.0.0.0:2888:3888;2181" --network=zoocluster --hostname=zoo3 -d --name=zookeeper3 zookeeper

To run zookeeper as single node in network mdc
----------------------------------------------
docker run --network mdc --name zoo -p 2181:2181 --hostname zoo -d zookeeper


To run mdc as separate node lanucher in network mdc
----------------------------------------------
docker network create --driver=bridge --subnet=172.30.0.0/16 --ip-range=172.30.0.0/16 mdc --attachable

docker run --network mdc --name namenode --hostname namenode -v "E:/DEVELOPMENT/dockershare:/opt/dockershare" -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" -e "CLUSTER_NAME=hadooptest" -p 9870:9870 -p 9000:9000 -d bde2020/hadoop-namenode

docker run --network mdc -v "E:/DEVELOPMENT/dockershare:/opt/dockershare" --hostname dnte --link namenode:namenode --link zoo:zoo -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "HDFS_CONF_dfs_namenode_datanode_registration_ip___hostname___check=false" --name mdccontainer --ip 172.30.0.20 -e ZKHOSTPORT=zoo:2181 -e HOST=172.30.0.20 -e PORT=10101 -e NODEPORT=12121 -p 12121:12121 --memory 4g -e MEMCONFIGLOW=-Xms512M -e MEMCONFIGHIGH=-Xmx512M -d arunsrajan/mdccontainer

To run task scheduler stream in network mdc
-------------------------------------------

docker run --network mdc -v "E:/DEVELOPMENT/dockershare:/opt/dockershare" --link mdccontainer:mdccontainer --link namenode:namenode --link zoo:zoo --hostname mdctss --name mdctaskschedulerstream --ip 172.30.0.22 -e ZKHOSTPORT=zoo:2181 -e HOST=172.30.0.22 -e PORT=22222 -p 22222:22222 -p 22223:22223 -e DEBUGPORT=*:4005 -p 4005:4005 --memory 3g  -e MEMCONFIGLOW=-Xms2G -e MEMCONFIGHIGH=-Xmx2G -d arunsrajan/mdctaskschedulerstream

To run task scheduler in network mdc
-------------------------------------------

docker run --network mdc -v "E:/DEVELOPMENT/dockershare:/opt/dockershare" --name mdctaskscheduler --link mdccontainer:mdccontainer --link namenode:namenode --link zoo:zoo --hostname mdcts --ip 172.30.0.23 -e ZKHOSTPORT=zoo:2181 -e HOST=172.30.0.23 -e PORT=11111 -p 11111:11111 -p 11112:11112 -e DPORT=*:4000 -p 4000:4000 --memory 3g -e MEMCONFIGLOW=-Xms2G -e MEMCONFIGHIGH=-Xmx2G -d arunsrajan/mdctaskscheduler

To run docker container as separate service in swarm using weave networks to support multicasting
----------------------------------------------------
docker plugin install weaveworks/net-plugin:latest_release
docker plugin disable weaveworks/net-plugin:latest_release
docker plugin set weaveworks/net-plugin:latest_release WEAVE_PASSWORD=
docker plugin set weaveworks/net-plugin:latest_release WEAVE_MULTICAST=1
docker plugin enable weaveworks/net-plugin:latest_release

sudo rm /run/docker/plugins/weave.sock
sudo rm /run/docker/plugins/weavemesh.sock

sudo curl -L git.io/weave -o /usr/local/bin/weave
sudo chmod a+x /usr/local/bin/weave

./weave connect 192.168.99.104

./weave connect 192.168.99.111

./weave connect 192.168.49.4

docker swarm init --advertise-addr 192.168.99.104 

docker swarm join --token SWMTKN-1-5g3bdtqdlqkgxjly8d66dbl2u28pd5cxt1haueo1eu0tc8a9j3-8zzmiks0lm6bar1n9r7y5pkfz 192.168.99.104:2377

docker network create --driver weaveworks/net-plugin:latest_release weave --attachable

docker service create --name zooweave --endpoint-mode dnsrr --network weave -d mesoscloud/zookeeper:3.4.8-centos-7

docker service update --publish-add published=2181,target=2181,protocol=tcp,mode=host zooweave

docker service create --name tesweave --endpoint-mode dnsrr --network weave -e HOST="{{.Service.Name}}.{{.Task.Slot}}.{{.Task.ID}}" -e ZKHOSTPORT=zooweave:2181 -e PORT=10101 -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx4G --replicas=3 arunsrajan/taskexecutorstream

docker service create --name tssweave --endpoint-mode vip -e MEMCONFIGLOW=-Xms1024M -e MEMCONFIGHIGH=-Xmx4096M --network weave -p 32325:22222 -e HOST="{{.Service.Name}}.{{.Task.Slot}}.{{.Task.ID}}" -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zooweave:2181 -e PORT=22222 arunsrajan/taskschedulerstream

docker service create --name tsweave --endpoint-mode vip --network weave -p 32326:11111 -e HOST="{{.Service.Name}}.{{.Task.Slot}}.{{.Task.ID}}" -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zooweave:2181 -e PORT=11111 arunsrajan/taskscheduler

docker service create --network weave --mount source=/dataset,target=/mnt/sftp/dataset,type=bind --name namenode --endpoint-mode dnsrr sequenceiq/hadoop-docker /etc/bootstrap.sh -bash

docker run --rm -e DAEMONS=namenode,datanode,secondarynamenode -v //d/sftp:/mnt/sftp/dataset --network weave --name=namenode -p 50070:50070 -p 50075:50075 -p 50090:50090 -p 9000:9000 cybermaggedon/hadoop:2.10.0 /start-namenode

docker run --rm -e DAEMONS=datanode --network weave --name=datanode --link namenode:namenode -e NAMENODE_URI=hdfs://namenode:9000 cybermaggedon/hadoop:2.10.0 /start-datanode


To remove dangling images
--------------------------

docker rmi -f $(docker images -f "dangling=true" -q)


To install pods in kubernetes and upload data.
----------------------------------------------


docker tag arunsrajan/mdctaskschedulerstream localhost:5000/mdctaskschedulerstream
docker tag arunsrajan/mdctaskscheduler localhost:5000/mdctaskscheduler
docker tag arunsrajan/mdccontainer localhost:5000/mdccontainer


docker push localhost:5000/mdctaskschedulerstream
docker push localhost:5000/mdctaskscheduler
docker push localhost:5000/mdccontainer


./hadoop dfs -mkdir /airlines

./hadoop dfs -chmod 777 /airlines

./hadoop dfs -mkdir /carriers

./hadoop dfs -chmod 777 /carriers

./hadoop dfs -put /mnt/sftp/dataset/dataset/1987.csv /airlines

./hadoop dfs -put /mnt/sftp/dataset/dataset/carriers.csv /carriers

./hadoop dfs -mkdir /mds

./hadoop dfs -chmod 777 /mds

./hadoop dfs -mkdir /newmapperout

./hadoop dfs -chmod 777 /newmapperout


To install weave scope
----------------------

sudo curl -L git.io/scope -o /usr/local/bin/scope
sudo chmod a+x /usr/local/bin/scope
scope launch



To run the project in openshift
-------------------------------
oc adm policy add-scc-to-user hostaccess developer --as system:admin
oc adm policy add-cluster-role-to-user cluster-admin developer --as system:admin


oc create -f persistvolume.yaml
oc create -f pervolumeclaim.yaml
oc create -f mdcnn.json
oc create -f mdctss.json
oc create -f mdczk.json
oc create -f pod.json

oc rsync D:/dataset/airlines namenode:/mnt/sftp/ -c hadoop-docker

cd /usr/local/hadoop/bin

./hadoop dfs -mkdir /airlines

./hadoop dfs -mkdir /carriers

./hadoop dfs -put /mnt/sftp/airlines/1987.csv /airlines

./hadoop dfs -put /mnt/sftp/airlines/1988.csv /airlines

./hadoop dfs -put /mnt/sftp/airlines/carriers.csv /carriers

./hadoop dfs -mkdir /mds

./hadoop dfs -chmod 777 /mds

./hadoop dfs -mkdir /newmapperout

./hadoop dfs -chmod 777 /newmapperout


To run the project in mesos
---------------------------

docker tag arunsrajan/mesos-master localhost:5000/mesos-master
docker tag arunsrajan/mesos-slave localhost:5000/mesos-slave

docker push localhost:5000/mesos-master
docker push localhost:5000/mesos-slave

docker network create --subnet=172.33.0.0/16 -d bridge mdcmesos

docker run --name zookeeper -p 2181:2181 -e ALLOW_ANONYMOUS_LOGIN=yes --network mdcmesos -d bitnami/zookeeper:3.5.7-debian-10-r23

docker run --name namenode --hostname namenode -v "D:/PROJECTS/hadoopspark:/opt/hadoopspark" --network=mdcmesos -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -e "CLUSTER_NAME=hadooptest" -p 9870:9870 -p 9000:9000 -d bde2020/hadoop-namenode

docker run --name datanode --hostname datanode --network=mdcmesos -e "CORE_CONF_fs_defaultFS=hdfs://namenode:9000" -d bde2020/hadoop-datanode

docker run --name mesos-master -p 5050:5050 -v "D:/PROJECTS/hadoopspark:/opt/hadoopspark" --ip 172.33.0.10 -p 22222:22222 --network mdcmesos -e MESOS_PORT=5050 -e MESOS_ZK=zk://zookeeper:2181/mesos -e MESOS_QUORUM=1 -e MESOS_REGISTRY=in_memory -e DEBUGPORT=*:4005 -p 4005:4005 -e MESOS_LOG_DIR=/var/log/mesos -e MESOS_WORK_DIR=/var/lib/mesos -e MESOS_HOSTNAME=172.33.0.10 -e MESOS_IP=172.33.0.10 -e MESOS_NATIVE_JAVA_LIBRARY=/usr/lib64/libmesos.so -e MEMCONFIGLOW=-Xms1G -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zookeeper:2181 -e PORT=22222 -e GCCCONFIG=-XX:+UseG1GC -e ISJGROUPS=false -e ISYARN=false -e ISMESOS=true -e MESOSMASTER=172.33.0.10:5050 -e HOST=172.33.0.10 -d arunsrajan/mesos-master

docker run --name mesos-slave --privileged -e GLOG_v=1 -v /var/run/docker.sock:/run/docker.sock -v "D:/PROJECTS/hadoopspark:/opt/hadoopspark" -p 5051:5051 --ip 172.33.0.11 --memory 10g --network mdcmesos -e MESOS_PORT=5051 -e MESOS_MASTER=zk://zookeeper:2181/mesos -e MESOS_QUORUM=1 -e MESOS_REGISTRY=in_memory -e MESOS_LOG_DIR=/var/log/mesos -e MESOS_LOGGING_LEVEL=INFO -e MESOS_WORK_DIR=/var/lib/mesos -e MESOS_NATIVE_JAVA_LIBRARY=/usr/lib64/libmesos.so -e MESOS_CONTAINERIZERS=mesos -d arunsrajan/mesos-slave

java -classpath ".:/opt/mdc/lib/*:/opt/mdc/modules/*" Deserialization
find /var -name "*stdout*" | grep Massive



To run the project in yarn
---------------------------

docker run --network mdc -v /dataset:/mnt/sftp/dataset -e DAEMONS=namenode,datanode,secondarynamenode --name=namenode -p 50070:50070 -p 50075:50075 -p 50090:50090 -p 9000:9000 -d cybermaggedon/hadoop:2.10.0 /start-namenode

docker run --network mdc -e DAEMONS=datanode -e NAMENODE_URI=hdfs://namenode:9000 --name=datanode --link namenode:namenode -P cybermaggedon/hadoop:2.10.0 /start-datanode 

docker run --network mdc --ip 172.18.0.91 --name=resourcemanager -p 8088:8088 -d  cybermaggedon/hadoop:2.10.0 /start-resourcemanager

docker run --network mdc --ip 172.18.0.92 -e RESOURCEMANAGER_HOSTNAME=resourcemanager --name=nodemanager --link resourcemanager:resourcemanager -d -P cybermaggedon/hadoop:2.10.0 /start-nodemanager


docker run --name yarntss -v /dataset:/mnt/sftp/dataset --ip 172.18.0.90 -p 22223:22222 --network mdc -e MEMCONFIGLOW=-Xms1G -e HDFSNN=hdfs://namenode:9000 -e HDFSRM=resourcemanager:8032 -e HDFSRS=resourcemanager:8030 -e MEMCONFIGHIGH=-Xmx2G -e ZKHOSTPORT=zookeeper:2181 -e PORT=22222 -e GCCCONFIG=-XX:+UseG1GC -e ISJGROUPS=false -e ISYARN=true -e ISMESOS=false -e HOST=172.18.0.90 -d arunsrajan/yarntss


Running Data Pipeline examples
------------------------------

Stream Reduce
------------
tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceNormalInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceNormalInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceNormalDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout


tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

Stream Reduce LOJ
-----------------

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceLeftOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceLeftOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceLeftOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceLeftOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceLeftOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout 1024 1 32

Stream Reduce ROJ
-----------------

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceRightOuterJoinIgnite  hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceRightOuterJoinJGroups  hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout


tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceRightOuterJoinNormal hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceRightOuterJoinYARN hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamReduceRightOuterJoinInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers /reduceout 1024 1 32

Stream Reduce SQL
-----------------

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumLocal hdfs://127.0.0.1:9000 /airline1989 /carriers 32 2

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumSAInMemory hdfs://127.0.0.1:9000 /airline1989 /carriers 16 2

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumSAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /carriers 16 2

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlSumSADisk hdfs://127.0.0.1:9000 /airline1989 /carriers 16 2

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlCountLocal hdfs://127.0.0.1:9000 /airline1989 16 2

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 64 2

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySADisk hdfs://127.0.0.1:9000 /airline1989 32 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemory hdfs://127.0.0.1:9000 /airline1989 32 1


tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountArrDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 32 1


tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelayLocal hdfs://127.0.0.1:9000 /airline1989 32 2

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySADisk hdfs://127.0.0.1:9000 /airline1989 32 1 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemory hdfs://127.0.0.1:9000 /airline1989 32 1 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.sql.examples.SqlUniqueCarrierSumCountDepDelaySAInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 32 1 1024 1

Stream Reduce Aggregate
-----------------------

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamAggSumCountArrDelayDisk hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamAggSumCountArrDelayIgnite hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamAggSumCountArrDelayInMemory hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamAggSumCountArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamAggSumCountArrDelayJGroups hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamAggSumCountArrDelayLocal hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamAggSumCountArrDelayYARN hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1


Filter Operation Streaming
------------------------

tasksubmitterstream.cmd ../modules/examples.jar com.github.mdc.stream.examples.tests.StreamFilterFilterCollectArrDelayInMemoryDisk hdfs://127.0.0.1:9000 /airline1989 /reduceout 1024 1 32


Running MR Job examples
------------------------

tasksubmitter.cmd -jar ../modules/examples.jar -args "com.github.mdc.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /mapreduceout 64"


tasksubmitter.cmd -jar ../modules/examples.jar -args "com.github.mdc.mr.examples.join.MrJobArrivalDelayYARN /airline1989 /carriers /mapreduceout 3 1024" 

tasksubmitter.cmd -jar ../modules/examples.jar -args "com.github.mdc.mr.examples.join.MrJobArrivalDelayIGNITE /airline1989 /carriers /mapreduceout"


Running Job In Linux

./tasksubmitter.sh -jar ../modules/examples.jar -args  'com.github.mdc.mr.examples.join.MrJobArrivalDelayNormal /airline1989 /carriers /mapreduceout 64'