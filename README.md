# Graphalytics Neo4j platform extension

[![Build Status](http://jenkins.tribler.org/buildStatus/icon?job=Graphalytics_Neo4j_master_tester)](http://jenkins.tribler.org/job/Graphalytics_Neo4j_master_tester/)


## Getting started

Please refer to the documentation of the Graphalytics core (`graphalytics` repository) for an introduction to using Graphalytics.


## Neo4j-specific benchmark configuration

The `neo4j` benchmark is run locally on the machine on which Graphalytics is launched. Before launching the benchmark, edit `config/neo4j.properties` and change the following settings:

- `jvm.heap.size.mb`: Set to the amount of heap space (in MB) to allocate to the Neo4j process.
- `neo4j.db.path`: Set to an appropriate path to store Neo4j databases in.

Other options in the `config/neo4j.properties` file are passed directly to Neo4j. This should be used to set e.g. buffer sizes.

