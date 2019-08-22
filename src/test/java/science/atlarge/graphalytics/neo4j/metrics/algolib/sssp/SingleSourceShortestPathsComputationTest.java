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
package science.atlarge.graphalytics.neo4j.metrics.algolib.sssp;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import science.atlarge.graphalytics.domain.algorithms.SingleSourceShortestPathsParameters;
import science.atlarge.graphalytics.neo4j.ValidationGraphLoader;
import science.atlarge.graphalytics.util.graph.PropertyGraph;
import science.atlarge.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsOutput;
import science.atlarge.graphalytics.validation.algorithms.sssp.SingleSourceShortestPathsValidationTest;

import java.util.Map;

/**
 * Test case for the single source shortest paths implementation on Neo4j.
 *
 * @author Tim Hegeman
 */
public class SingleSourceShortestPathsComputationTest extends SingleSourceShortestPathsValidationTest {

    @Override
    public SingleSourceShortestPathsOutput executeDirectedSingleSourceShortestPaths(
            PropertyGraph<Void, Double> graph,
            SingleSourceShortestPathsParameters parameters) throws KernelException {
        return execute(graph, parameters, true);

    }

    @Override
    public SingleSourceShortestPathsOutput executeUndirectedSingleSourceShortestPaths(
            PropertyGraph<Void, Double> graph,
            SingleSourceShortestPathsParameters parameters) throws KernelException {
        return execute(graph, parameters, false);
    }

    private SingleSourceShortestPathsOutput execute(PropertyGraph<Void, Double> graph,
                                                    SingleSourceShortestPathsParameters parameters,
                                                    boolean directed) throws KernelException {
        GraphDatabaseService graphDatabase = ValidationGraphLoader.loadValidationGraphToDatabase(graph);

        Map<Long, Double> pathLengths =
                new SingleSourceShortestPathsComputation(graphDatabase, parameters.getSourceVertex(), directed).run();
        return new SingleSourceShortestPathsOutput(pathLengths);
    }

}
