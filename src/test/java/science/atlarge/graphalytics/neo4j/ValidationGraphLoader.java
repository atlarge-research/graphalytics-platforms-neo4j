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

import org.neo4j.graphdb.*;
import org.neo4j.test.TestGraphDatabaseFactory;
import science.atlarge.graphalytics.util.graph.PropertyGraph;
import science.atlarge.graphalytics.validation.GraphStructure;

import java.util.HashMap;
import java.util.Map;

import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.*;
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


	public static <V, E> GraphDatabaseService loadValidationGraphToDatabase(PropertyGraph<V, E> graph) {
		GraphDatabaseService graphDatabase = new TestGraphDatabaseFactory().newImpermanentDatabase();
		try (Transaction tx = graphDatabase.beginTx()) {
			Map<PropertyGraph<V, E>.Vertex, Node> vertexToNode = new HashMap<>();
			for (PropertyGraph<V, E>.Vertex vertex : graph.getVertices()) {
				Node newVertex = graphDatabase.createNode((Label)Vertex);
				newVertex.setProperty(ID_PROPERTY, vertex.getId());
				vertexToNode.put(vertex, newVertex);
			}

			for (PropertyGraph<V, E>.Vertex vertex : graph.getVertices()) {
				Node sourceVertex = vertexToNode.get(vertex);

				for (PropertyGraph<V, E>.Edge edge : vertex.getOutgoingEdges()) {
					PropertyGraph<V, E>.Vertex destinationVertex = edge.getDestinationVertex();
					Relationship rel = sourceVertex.createRelationshipTo(vertexToNode.get(destinationVertex), EDGE);
					rel.setProperty(WEIGHT_PROPERTY, edge.getValue());
				}
			}

			tx.success();
		}
		return graphDatabase;
	}

}
