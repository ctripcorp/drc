#!/bin/sh

docker pull mysql:5.7

mkdir -p /data/drc/replicator
chmod a+w /data/drc/replicator

mvn clean test -pl :integration-test -Dtest=BidirectionalStarter