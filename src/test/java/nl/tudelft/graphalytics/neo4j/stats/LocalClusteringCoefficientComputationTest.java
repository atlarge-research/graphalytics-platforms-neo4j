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
package nl.tudelft.graphalytics.neo4j.stats;

import nl.tudelft.graphalytics.neo4j.ValidationGraphLoader;
import nl.tudelft.graphalytics.neo4j.stats.LocalClusteringCoefficientComputation.LocalClusteringCoefficientResult;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.stats.LocalClusteringCoefficientOutput;
import nl.tudelft.graphalytics.validation.stats.LocalClusteringCoefficientValidationTest;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.HashMap;
import java.util.Map;

import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static nl.tudelft.graphalytics.neo4j.stats.LocalClusteringCoefficientComputation.LCC;

/**
 * Test case for the connected components implementation on Neo4j.
 *
 * @author Tim Hegeman
 */
public class LocalClusteringCoefficientComputationTest extends LocalClusteringCoefficientValidationTest {

	@Override
	public LocalClusteringCoefficientOutput executeDirectedLocalClusteringCoefficient(GraphStructure graph)
			throws Exception {
		return executeLocalClusteringCoefficient(graph);
	}

	@Override
	public LocalClusteringCoefficientOutput executeUndirectedLocalClusteringCoefficient(GraphStructure graph)
			throws Exception {
		return executeLocalClusteringCoefficient(graph);
	}

	private LocalClusteringCoefficientOutput executeLocalClusteringCoefficient(GraphStructure graph) {
		GraphDatabaseService database = ValidationGraphLoader.loadValidationGraphToDatabase(graph);
		LocalClusteringCoefficientResult result = new LocalClusteringCoefficientComputation(database).run();

		Map<Long, Double> output = new HashMap<>();
		try (Transaction ignored = database.beginTx()) {
			for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
				output.put((long)node.getProperty(ID_PROPERTY), (double)node.getProperty(LCC));
			}
		}
		database.shutdown();
		return new LocalClusteringCoefficientOutput(output, result.getMeanLcc());
	}

}
