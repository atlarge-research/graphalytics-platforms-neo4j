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
package science.atlarge.graphalytics.neo4j.algolib.pr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphalgo.PageRankProc;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.exceptions.KernelException;
import science.atlarge.graphalytics.neo4j.Neo4jTransactionManager;
import science.atlarge.graphalytics.neo4j.algolib.AlgoLibHelper;

/**
 * Implementation of the PageRank algorithm in Neo4j. This class is responsible for the computation,
 * given a functional Neo4j database instance.
 *
 * @author Tim Hegeman
 */
public class PageRankComputation {

    private static final Logger LOG = LogManager.getLogger();

    public static final String PAGERANK = "PAGERANK";

    private final GraphDatabaseService graphDatabase;
    private final int maxIterations;
    private final float dampingFactor;

    /**
     * @param graphDatabase graph database representing the input graph
     * @param maxIterations maximum number of iterations of the PageRank algorithm to run
     * @param dampingFactor the damping factor parameter for the PageRank algorithm
     */
    public PageRankComputation(GraphDatabaseService graphDatabase, int maxIterations, float dampingFactor) throws KernelException {
        this.graphDatabase = graphDatabase;
        this.maxIterations = maxIterations;
        this.dampingFactor = dampingFactor;

        AlgoLibHelper.registerProcedure(graphDatabase, PageRankProc.class);
    }

    /**
     * Executes the PageRank algorithm by setting the PAGERANK property on all nodes.
     */
    public void run() {
        LOG.debug("- Starting PageRank algorithm");
        try (Neo4jTransactionManager transactionManager = new Neo4jTransactionManager(graphDatabase)) {
            final String command = String.format("" +
                            "CALL algo.pageRank(null, null,\n" +
                            "  {write: true, writeProperty: '%s', iterations: %d, dampingFactor: %f}\n" +
                            ")\n" +
                            "YIELD nodes, iterations, loadMillis, computeMillis, writeMillis, dampingFactor, write, writeProperty",
                    PAGERANK,
                    maxIterations,
                    dampingFactor
            );
            graphDatabase.execute(command);
        }
        LOG.debug("- Completed PageRank algorithm");
    }

}
