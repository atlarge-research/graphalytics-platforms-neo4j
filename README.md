# Graphalytics Neo4j platform driver

Neo4j implementation of the LDBC Graphalytics benchmark. This repository contains two sets of implementations:

* `embedded` uses Neo4j's Java API, available in [embedded mode](https://neo4j.com/docs/java-reference/current/tutorials-java-embedded/)
* `algolib` use the [Neo4j Graph Algorithms Library](https://neo4j.com/docs/graph-algorithms/current/)

To run the benchmark, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the Neo4j-specific instructions listed below.

### Configuring and running the benchmark

To initialize the benchmark package, run:

```bash
./init.sh MY_GRAPH_DIR
```

where `MY_GRAPH_DIR` should point to the directory of the graphs and the validation data. The default value is `~/graphs`.

Additionally, in `config/platform.properties`:
* Set the `platform.neo4j.home` value to your Neo4j home directory, e.g., `/home/ubuntu/neo4j`
* Set the `benchmark.impl` value to `embedded` or `algolib`.
