/*
 * Copyright 2015 Delft University of Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package science.atlarge.graphalytics.neo4j.algolib.sssp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphalgo.ShortestPathProc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import science.atlarge.graphalytics.neo4j.Neo4jConfiguration;
import science.atlarge.graphalytics.neo4j.Neo4jTransactionManager;
import science.atlarge.graphalytics.neo4j.algolib.AlgoLibHelper;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of the connected components algorithm in Neo4j. This class is responsible for the computation,
 * given a functional Neo4j database instance.
 *
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathsComputation {

    private static final Logger LOG = LogManager.getLogger();

    public static final String SSSP = "SSSP";
    private final GraphDatabaseService graphDatabase;
    private final long startVertexId;
    private final boolean directed;

    /**
     * @param graphDatabase graph database representing the input graph
     */
    public SingleSourceShortestPathsComputation(
            GraphDatabaseService graphDatabase,
            long startVertexId,
            boolean directed
    ) throws KernelException {
        this.graphDatabase = graphDatabase;
        this.startVertexId = startVertexId;
        this.directed = directed;

        AlgoLibHelper.registerProcedure(graphDatabase, ShortestPathProc.class);
    }

    /**
     * Executes the connected components algorithm by setting the SSSP property of all nodes to the smallest node
     * ID in each component.
     */
    public Map<Long, Double> run() {
        LOG.debug("- Starting Single Source Shortest Paths algorithm");
        Map<Long, Double> costs = new HashMap<>();
        try (Neo4jTransactionManager transactionManager = new Neo4jTransactionManager(graphDatabase)) {
            final String command = String.format("" +
                            "MATCH (startNode {%s: %d}), (endNode)\n" +
                            "CALL algo.shortestPath(startNode, endNode, '%s',\n" +
                            "  {write: true, writeProperty: '%s', direction: '%s'}\n" +
                            ")\n" +
                            "YIELD nodeCount, totalCost, loadMillis, evalMillis, writeMillis\n" +
                            "RETURN\n" +
                            "  endNode.%s as vertex,\n" +
                            "  CASE \n" +
                            "    WHEN totalCost = -1 AND startNode = endNode THEN 0.0\n" +
                            "    ELSE toFloat(totalCost)\n" +
                            "  END AS cost",
                    Neo4jConfiguration.ID_PROPERTY,
                    startVertexId,
                    Neo4jConfiguration.WEIGHT_PROPERTY,
                    SSSP,
                    directed ? "OUTGOING" : "BOTH",
                    Neo4jConfiguration.ID_PROPERTY
            );
            final Result result = graphDatabase.execute(command);
            while (result.hasNext()) {
                final Map<String, Object> row = result.next();
                final long vertex = (long) row.get("vertex");
                final double cost = (double) row.get("cost");
                costs.put(vertex, cost >= 0 ? cost : Double.POSITIVE_INFINITY);
            }
        }
        LOG.debug("- Completed Single Source Shortest Paths algorithm");
        return costs;
    }

}
