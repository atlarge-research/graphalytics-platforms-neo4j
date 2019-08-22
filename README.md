# Graphalytics Neo4j platform driver

Neo4j... (TODO: provide a short description on Neo4j). To execute Graphalytics benchmark on Neo4j, follow the steps in the Graphalytics tutorial on [Running Benchmark](https://github.com/ldbc/ldbc_graphalytics/wiki/Manual%3A-Running-Benchmark) with the Neo4j-specific instructions listed below.

### Obtain the platform driver
There are two possible ways to obtain the Neo4j platform driver:

 1. **Download the (prebuilt) [Neo4j platform driver](http://graphalytics.site/dist/stable/) distribution from our website.

 2. **Build the platform drivers**: 
- Download the source code from this repository.
- Execute `mvn clean package` in the root directory (See details in [Software Build](https://github.com/ldbc/ldbc_graphalytics/wiki/Documentation:-Software-Build)).
- Extract the distribution from  `graphalytics-{graphalytics-version}-neo4j-{platform-version}.tar.gz`.

### Configure the benchmark

To initialize the benchmark package, run:

```bash
./init.sh MY_GRAPH_DIR
```

where `MY_GRAPH_DIR` should point to the directory of the graphs and the validation data. The default value is `~/graphs`.

Additionally, in `config/platform.properties`:
* Set the `platform.neo4j.home` value to your Neo4j home directory, e.g., `/home/ubuntu/neo4j`
* Set the `benchmark.impl` value to `embedded` or `algolib`.
