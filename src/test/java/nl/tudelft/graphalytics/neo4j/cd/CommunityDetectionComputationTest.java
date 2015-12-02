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
package nl.tudelft.graphalytics.neo4j.cd;

import nl.tudelft.graphalytics.domain.algorithms.CommunityDetectionParameters;
import nl.tudelft.graphalytics.neo4j.ValidationGraphLoader;
import nl.tudelft.graphalytics.validation.GraphStructure;
import nl.tudelft.graphalytics.validation.cd.CommunityDetectionOutput;
import nl.tudelft.graphalytics.validation.cd.CommunityDetectionValidationTest;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;

import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static nl.tudelft.graphalytics.neo4j.cd.CommunityDetectionComputation.LABEL;

/**
 * Test case for the community detection implementation on Neo4j.
 *
 * @author Tim Hegeman
 */
public class CommunityDetectionComputationTest extends CommunityDetectionValidationTest {

	@Override
	public CommunityDetectionOutput executeDirectedCommunityDetection(GraphStructure graph,
			CommunityDetectionParameters parameters) throws Exception {
		return executeCommunityDetection(graph, parameters);
	}

	@Override
	public CommunityDetectionOutput executeUndirectedCommunityDetection(GraphStructure graph,
			CommunityDetectionParameters parameters) throws Exception {
		return executeCommunityDetection(graph, parameters);
	}

	private CommunityDetectionOutput executeCommunityDetection(GraphStructure graph,
			CommunityDetectionParameters parameters) {
		GraphDatabaseService database = ValidationGraphLoader.loadValidationGraphToDatabase(graph);
		new CommunityDetectionComputation(database, parameters.getNodePreference(), parameters.getHopAttenuation(),
				parameters.getMaxIterations()).run();

		Map<Long, Long> output = new HashMap<>();
		try (Transaction ignored = database.beginTx()) {
			for (Node node : GlobalGraphOperations.at(database).getAllNodes()) {
				output.put((long)node.getProperty(ID_PROPERTY), (long)node.getProperty(LABEL));
			}
		}
		database.shutdown();
		return new CommunityDetectionOutput(output);
	}

}
