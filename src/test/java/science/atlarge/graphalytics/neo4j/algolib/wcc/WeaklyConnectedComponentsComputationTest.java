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
package science.atlarge.graphalytics.neo4j.algolib.wcc;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import science.atlarge.graphalytics.neo4j.ValidationGraphLoader;
import science.atlarge.graphalytics.validation.GraphStructure;
import science.atlarge.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsOutput;
import science.atlarge.graphalytics.validation.algorithms.wcc.WeaklyConnectedComponentsValidationTest;

import java.util.HashMap;
import java.util.Map;

import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static science.atlarge.graphalytics.neo4j.algorithms.wcc.WeaklyConnectedComponentsComputation.COMPONENT;

/**
 * Test case for the weakly connected components (WCC) implementation on Neo4j. Unlike SCC, WCC does not consider the
 * direction of edges, so directed and undirected tests invoke the same methods.
 *
 * @author Tim Hegeman
 */
public class WeaklyConnectedComponentsComputationTest extends WeaklyConnectedComponentsValidationTest {

	@Override
	public WeaklyConnectedComponentsOutput executeDirectedConnectedComponents(GraphStructure graph) throws KernelException {
		return executeConnectedComponents(graph);
	}

	@Override
	public WeaklyConnectedComponentsOutput executeUndirectedConnectedComponents(GraphStructure graph) throws KernelException {
		return executeConnectedComponents(graph);
	}

	private WeaklyConnectedComponentsOutput executeConnectedComponents(GraphStructure graph) throws KernelException {
		GraphDatabaseService database = ValidationGraphLoader.loadValidationGraphToDatabase(graph);
		new WeaklyConnectedComponentsComputation(database).run();

		Map<Long, Long> output = new HashMap<>();
		try (Transaction ignored = database.beginTx()) {
			for (Node node : database.getAllNodes()) {
				output.put((long)node.getProperty(ID_PROPERTY), Long.valueOf((int)node.getProperty(COMPONENT)));
			}
		}
		database.shutdown();
		return new WeaklyConnectedComponentsOutput(output);
	}

}
