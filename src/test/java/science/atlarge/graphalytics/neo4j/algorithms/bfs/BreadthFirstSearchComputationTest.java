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
package science.atlarge.graphalytics.neo4j.algorithms.bfs;

import science.atlarge.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import science.atlarge.graphalytics.neo4j.ValidationGraphLoader;
import science.atlarge.graphalytics.validation.GraphStructure;
import science.atlarge.graphalytics.validation.algorithms.bfs.BreadthFirstSearchOutput;
import science.atlarge.graphalytics.validation.algorithms.bfs.BreadthFirstSearchValidationTest;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.Map;

import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static science.atlarge.graphalytics.neo4j.algorithms.bfs.BreadthFirstSearchComputation.DISTANCE;

/**
 * Test case for the breadth-first search implementation on Neo4j.
 *
 * @author Tim Hegeman
 */
public class BreadthFirstSearchComputationTest extends BreadthFirstSearchValidationTest {

	@Override
	public BreadthFirstSearchOutput executeDirectedBreadthFirstSearch(GraphStructure graph,
			BreadthFirstSearchParameters parameters) throws Exception {
		return executeBreadthFirstSearch(graph, parameters, true);
	}

	@Override
	public BreadthFirstSearchOutput executeUndirectedBreadthFirstSearch(GraphStructure graph,
			BreadthFirstSearchParameters parameters) throws Exception {
		return executeBreadthFirstSearch(graph, parameters, true);
	}

	private BreadthFirstSearchOutput executeBreadthFirstSearch(GraphStructure graph,
			BreadthFirstSearchParameters parameters, boolean directed) {
		GraphDatabaseService database = ValidationGraphLoader.loadValidationGraphToDatabase(graph);
		new BreadthFirstSearchComputation(database, parameters.getSourceVertex(), directed).run();

		Map<Long, Long> output = new HashMap<>();
		try (Transaction ignored = database.beginTx()) {
			for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
				if (node.hasProperty(DISTANCE)) {
					output.put((long)node.getProperty(ID_PROPERTY), (long)node.getProperty(DISTANCE));
				} else {
					output.put((long)node.getProperty(ID_PROPERTY), Long.MAX_VALUE);
				}
			}
		}
		database.shutdown();
		return new BreadthFirstSearchOutput(output);
	}

}
