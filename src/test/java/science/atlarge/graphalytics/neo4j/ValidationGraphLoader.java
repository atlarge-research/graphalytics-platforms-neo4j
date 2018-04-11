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
package science.atlarge.graphalytics.neo4j;

import science.atlarge.graphalytics.validation.GraphStructure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;

import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.EDGE;
import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.VertexLabelEnum.Vertex;

/**
 * Utility class for loading a validation graph into a Neo4j in-memory database.
 *
 * @author Tim Hegeman
 */
public final class ValidationGraphLoader {

	private ValidationGraphLoader() {
	}

	public static GraphDatabaseService loadValidationGraphToDatabase(GraphStructure validationGraph) {
		GraphDatabaseService graphDatabase = new TestGraphDatabaseFactory().newImpermanentDatabase();
		try (Transaction tx = graphDatabase.beginTx()) {
			Map<Long, Node> vertexToNode = new HashMap<>();
			for (long vertexId : validationGraph.getVertices()) {
				Node newVertex = graphDatabase.createNode((Label)Vertex);
				newVertex.setProperty(ID_PROPERTY, vertexId);
				vertexToNode.put(vertexId, newVertex);
			}

			for (long vertexId : validationGraph.getVertices()) {
				Node sourceVertex = vertexToNode.get(vertexId);
				for (long neighbourId : validationGraph.getEdgesForVertex(vertexId)) {
					sourceVertex.createRelationshipTo(vertexToNode.get(neighbourId), EDGE);
				}
			}

			tx.success();
		}
		return graphDatabase;
	}

}