#!/bin/bash

########################################################################
#
# Copyright (c) 2011 Yahoo! Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License. See accompanying LICENSE file.
#
########################################################################



BUFFERSIZE=1000;
BATCHSIZE=1000;
#these could be overridden by env.sh
BKPARAM1=4
BKPARAM2=2

SCRIPTDIR=`dirname $0`
cd $SCRIPTDIR;
#TODO: we do not need env.sh, the parameters could be read from omid-site.xml
source ../../env.sh
CLASSPATH=../conf
for j in ../target/omid*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

for j in ../lib/*.jar; do
    CLASSPATH=$CLASSPATH:$j
done

if which greadlink; then
	READLINK=greadlink
else
	READLINK=readlink
fi

# run a test of tso in isolation from hbase
tsobench() {
NMSG=10
NCLIENTS=5
MAX_ROWS=20
if [ $# -ge 1 ]; then
NMSG=$1
fi
if [ $# -ge 2 ]; then
NCLIENTS=$2
fi
if [ $# -ge 3 ]; then
MAX_ROWS=$3
fi

echo running with $NMSG outstanding messages and $NCLIENTS clients
echo MAX_ROWS = $MAX_ROWS
simclients localhost $NMSG $NCLIENTS $MAX_ROWS
}

# use to run tso experiments with simulated clients
simclients() {
    export LD_LIBRARY_PATH=`readlink -f ../src/main/native`
	 machine=$1
	 shift
#YOURKIT="-Xdebug -Xrunjdwp:transport=dt_socket,address=5005,server=y,suspend=n"
    exec $JAVA_HOME/bin/java $YOURKIT -Xmx2048m -cp $CLASSPATH -Djava.library.path=$LD_LIBRARY_PATH -Domid.maxItems=1000000 -Domid.maxCommits=30000000 -Dlog4j.configuration=log4j.properties com.yahoo.omid.client.SimClient $TSOSERVER0 1234 -zk $ZKSERVERLIST 1000000000 $*   &> ${STATS}/clients-$machine.A
}

sequencer() {
	ID=-1;
	PORT=1233;
	if [ $# -ge 1 ]; then
		PORT=$1
	fi
    export LD_LIBRARY_PATH=`$READLINK -f ../src/main/native`
    exec $JAVA_HOME/bin/java $YOURKIT -Xmx1024m -cp $CLASSPATH -Djava.library.path=$LD_LIBRARY_PATH -Domid.maxItems=1000000 -Domid.maxCommits=30000000 -Dlog4j.configuration=log4j.properties com.yahoo.omid.tso.SequencerServer -port $PORT -batch $BATCHSIZE -ensemble $BKPARAM1 -quorum $BKPARAM2 -zk $ZKSERVERLIST -soId $ID &> $STATS/sequencer.log
}

tso() {
	ID=0;
	if [ $# -ge 1 ]; then
		ID=$1
	fi
#YOURKIT="-agentpath:/net/yrl-scratch/maysam/profiler/yjp-9.0.9/bin/linux-x86-64/libyjpagent.so=disablestacktelemetry,disableexceptiontelemetry,builtinprobes=none,delay=10000"
    export LD_LIBRARY_PATH=`$READLINK -f ../src/main/native`
    exec $JAVA_HOME/bin/java $YOURKIT -Xmx1024m -cp $CLASSPATH -Djava.library.path=$LD_LIBRARY_PATH -Domid.maxItems=1000000 -Domid.maxCommits=30000000 -Dlog4j.configuration=log4j.properties com.yahoo.omid.tso.TSOServer   -port 1234 -batch $BATCHSIZE -ensemble $BKPARAM1 -quorum $BKPARAM2 -zk $ZKSERVERLIST -soId $ID &> $STATS/tso$ID.log
}

bktest() {
    exec java -cp $CLASSPATH -Dlog4j.configuration=log4j.properties org.apache.bookkeeper.util.LocalBookKeeper 5
}

tranhbase() {
    pwd
    echo $CLASSPATH
    exec java -cp $CLASSPATH org.apache.hadoop.hbase.LocalHBaseCluster 
}

testtable() {
    exec java -cp $CLASSPATH:../target/test-classes com.yahoo.omid.TestTable
}

usage() {
    echo "Usage: omid.sh <command>"
    echo "where <command> is one of:"
    echo "  tso           Start the timestamp oracle server."
    echo "  tsobench      Run a simple benchmark of the TsO."
    echo "  bktest        Start test bookkeeper ensemble. Starts zookeeper also."
    echo "  tran-hbase    Start hbase with transaction support."
    echo "  test-table    Create test table"
}

# if no args specified, show usage
if [ $# = 0 ]; then
    usage;
    exit 1
fi

COMMAND=$1

if [ "$COMMAND" = "tso" ]; then
	 shift
    tso $*;
elif [ "$COMMAND" = "sequencer" ]; then
  shift
	 sequencer $*;
elif [ "$COMMAND" = "simclients" ]; then
	 shift
    simclients $*;
elif [ "$COMMAND" = "tsobench" ]; then
    tsobench;
elif [ "$COMMAND" = "bktest" ]; then
    bktest;
elif [ "$COMMAND" = "tran-hbase" ]; then
    tranhbase;
elif [ "$COMMAND" = "test-table" ]; then
    testtable;
else
    usage;
fi


