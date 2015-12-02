package nl.tudelft.graphalytics.neo4j;

import nl.tudelft.graphalytics.validation.GraphStructure;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.HashMap;
import java.util.Map;

import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.EDGE;
import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.VertexLabelEnum.Vertex;

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
