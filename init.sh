#/bin/sh

set -e

GRAPHS_DIR=${1:-~/graphs}
NEO4J_DIR=${2:-~/neo4j}
IMPLEMENTATION=${3:-algolib}

PROJECT=graphalytics-1.0.0-neo4j-0.1-SNAPSHOT

rm -rf $PROJECT
mvn package -DskipTests
tar xf $PROJECT-bin.tar.gz
cd $PROJECT/
cp -r config-template config
sed -i "s|^graphs.root-directory =$|graphs.root-directory = $GRAPHS_DIR|g" config/benchmark.properties
sed -i "s|^graphs.validation-directory =$|graphs.validation-directory = $GRAPHS_DIR|g" config/benchmark.properties
sed -i "s|^platform.neo4j.home =$|platform.neo4j.home = $NEO4J_DIR|g" config/platform.properties
sed -i "s|^platform.impl =$|platform.impl = $IMPLEMENTATION|g" config/platform.properties
