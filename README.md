# Graphalytics Neo4j platform driver

Neo4j implementation of the LDBC Graphalytics benchmark. This repository contains two sets of implementations:

* `embedded` uses Neo4j's Java API, available in [embedded mode](https://neo4j.com/docs/java-reference/current/tutorials-java-embedded/)
* `algolib` use the [Neo4j Graph Algorithms Library](https://neo4j.com/docs/graph-algorithms/current/)

To run the benchmark, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the Neo4j-specific instructions listed below.

### Configuring and running the benchmark

To initialize the benchmark package, run:

```bash
./init.sh MY_GRAPH_DIR NEO4J_DIR IMPLEMENTATION
```

where

* `MY_GRAPH_DIR` should point to the directory of the graphs and the validation data. The default value is `~/graphs`.
* `NEOJ4_DIR` should point to Neo4j's directory. The default value is `~/neo4j`.
* `IMPLEMENTATION` selects the implementation to use (`embedded` or `algolib`). The default value is `algolib`.

You can later review these configurations are set in `config/benchmark.properties` and `config/platform.properties`.
