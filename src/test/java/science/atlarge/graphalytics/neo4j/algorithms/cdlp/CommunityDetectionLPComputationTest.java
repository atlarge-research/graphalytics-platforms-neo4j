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
package science.atlarge.graphalytics.neo4j.algorithms.cdlp;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import science.atlarge.graphalytics.domain.algorithms.CommunityDetectionLPParameters;
import science.atlarge.graphalytics.neo4j.ValidationGraphLoader;
import science.atlarge.graphalytics.validation.GraphStructure;
import science.atlarge.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPOutput;
import science.atlarge.graphalytics.validation.algorithms.cdlp.CommunityDetectionLPValidationTest;

import java.util.HashMap;
import java.util.Map;

import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static science.atlarge.graphalytics.neo4j.algorithms.cdlp.CommunityDetectionLPComputation.LABEL;

/**
 * Test case for the community detection implementation on Neo4j.
 *
 * @author Tim Hegeman
 */
public class CommunityDetectionLPComputationTest extends CommunityDetectionLPValidationTest {

	@Override
	public CommunityDetectionLPOutput executeDirectedCommunityDetection(GraphStructure graph,
			CommunityDetectionLPParameters parameters) {
		return executeCommunityDetection(graph, parameters);
	}

	@Override
	public CommunityDetectionLPOutput executeUndirectedCommunityDetection(GraphStructure graph,
			CommunityDetectionLPParameters parameters) {
		return executeCommunityDetection(graph, parameters);
	}

	private CommunityDetectionLPOutput executeCommunityDetection(GraphStructure graph,
			CommunityDetectionLPParameters parameters) {
		GraphDatabaseService database = ValidationGraphLoader.loadValidationGraphToDatabase(graph);
		new CommunityDetectionLPComputation(database, parameters.getMaxIterations()).run();

		Map<Long, Long> output = new HashMap<>();
		try (Transaction ignored = database.beginTx()) {
			for (Node node : database.getAllNodes()) {
				output.put((long)node.getProperty(ID_PROPERTY), (long)node.getProperty(LABEL));
			}
		}
		database.shutdown();
		return new CommunityDetectionLPOutput(output);
	}

}
