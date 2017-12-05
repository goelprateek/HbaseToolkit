#!/usr/bin/env bash


export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`hbase classpath`

$HADOOP_HOME/bin/yarn jar target/mongodb-hbase-connector-1.0-SNAPSHOT-with-deps.jar \
    --mongo mongodb://localhost:37017/?readPreference=secondary \
    --database mydb \
    --collection foo \
    --merge

