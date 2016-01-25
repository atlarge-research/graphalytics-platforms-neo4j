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
package nl.tudelft.graphalytics.neo4j.algorithms.ffm;

import nl.tudelft.graphalytics.domain.algorithms.ForestFireModelParameters;
import nl.tudelft.graphalytics.neo4j.Neo4jConfiguration;
import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;

import java.util.*;

/**
 * Implementation of the forest fire model for graph evolution using Neo4j.
 *
 * @author Tim Hegeman
 */
public class ForestFireModelComputation {

	public static final String INITIAL_VERTEX = "INITVERTEX";
	private final GraphDatabaseService graphDatabase;
	private final ForestFireModelParameters parameters;
	private final boolean directedGraph;
	private final Random random;

	/**
	 * @param graphDatabase graph database representing the input graph
	 * @param parameters    parameters for the forest fire model
	 * @param directedGraph true iff the input graph is directed
	 */
	public ForestFireModelComputation(GraphDatabaseService graphDatabase, ForestFireModelParameters parameters,
			boolean directedGraph) {
		this.graphDatabase = graphDatabase;
		this.parameters = parameters;
		this.directedGraph = directedGraph;
		this.random = new Random();
	}

	/**
	 * Executes the forest fire model algorithm by creating new vertices and edges according to the model and the
	 * provided parameters. For each new vertex a random vertex is chosen to be the "ambassador", or initial vertex,
	 * to which an edge is formed. The id of this ambassador is stored as INITIAL_VERTEX property of the new vertex.
	 * Next, edges are formed to a random set of out- and in-neighbours for a specified number of iterations.
	 */
	public void run() {
		try (Transaction transaction = graphDatabase.beginTx()) {
			for (long newVertexId = parameters.getMaxId() + 1;
			     newVertexId <= parameters.getMaxId() + parameters.getNumNewVertices();
			     newVertexId++) {
				Node ambassador = chooseAmbassador();
				Node newVertex = createNewVertex(newVertexId, ambassador);
				propagateFire(newVertex, ambassador);
			}
			transaction.success();
		}
	}

	private void propagateFire(Node newVertex, Node ambassador) {
		Set<Node> burntVertices = new HashSet<>();
		Set<Node> burningVertices = new HashSet<>();
		burningVertices.add(ambassador);
		for (int distance = 0; distance < parameters.getMaxIterations(); distance++) {
			Set<Node> newBurningVertices = new HashSet<>();
			for (Node burningVertex : burningVertices) {
				selectOutLinks(newVertex, burningVertex, burntVertices, newBurningVertices);
				selectInLinks(newVertex, burningVertex, burntVertices, newBurningVertices);
			}
			for (Node newBurningVertex : newBurningVertices) {
				newVertex.createRelationshipTo(newBurningVertex, Neo4jConfiguration.EDGE);
				if (!directedGraph) {
					newBurningVertex.createRelationshipTo(newVertex, Neo4jConfiguration.EDGE);
				}
			}
			burntVertices.addAll(newBurningVertices);
			burningVertices = newBurningVertices;
		}
	}

	private void selectOutLinks(Node newVertex, Node burningVertex, Set<Node> burntVertices,
			Set<Node> newBurningVertices) {
		int count = randomGeometric(parameters.getPRatio());
		List<Node> eligibleNeighbours = new ArrayList<>();
		for (Relationship edge : burningVertex.getRelationships(Neo4jConfiguration.EDGE, Direction.OUTGOING)) {
			if (edge.getEndNode().getId() != newVertex.getId() && !burntVertices.contains(edge.getEndNode()) &&
					!newBurningVertices.contains(edge.getEndNode())) {
				eligibleNeighbours.add(edge.getEndNode());
			}
		}
		Collections.shuffle(eligibleNeighbours, random);
		newBurningVertices.addAll(eligibleNeighbours.subList(0, Math.min(count, eligibleNeighbours.size())));
	}

	private void selectInLinks(Node newVertex, Node burningVertex, Set<Node> burntVertices,
			Set<Node> newBurningVertices) {
		int count = randomGeometric(parameters.getRRatio());
		List<Node> eligibleNeighbours = new ArrayList<>();
		for (Relationship edge : burningVertex.getRelationships(Neo4jConfiguration.EDGE, Direction.INCOMING)) {
			if (edge.getEndNode().getId() != newVertex.getId() && !burntVertices.contains(edge.getEndNode()) &&
					!newBurningVertices.contains(edge.getEndNode())) {
				eligibleNeighbours.add(edge.getEndNode());
			}
		}
		Collections.shuffle(eligibleNeighbours, random);
		newBurningVertices.addAll(eligibleNeighbours.subList(0, Math.min(count, eligibleNeighbours.size())));
	}

	private int randomGeometric(float geometricParameter) {
		if (geometricParameter <= 0.0f)
			return Integer.MAX_VALUE;
		if (geometricParameter >= 1.0f)
			return 0;
		return (int)(Math.log(random.nextFloat()) / Math.log(1.0f - geometricParameter));
	}

	private Node createNewVertex(long newVertexId, Node ambassador) {
		// Create the new vertex and an edge to the ambassador
		Node newNode = graphDatabase.createNode();
		newNode.setProperty(Neo4jConfiguration.ID_PROPERTY, newVertexId);
		newNode.setProperty(INITIAL_VERTEX, ambassador.getProperty(Neo4jConfiguration.ID_PROPERTY));
		newNode.createRelationshipTo(ambassador, Neo4jConfiguration.EDGE);
		if (!directedGraph) {
			ambassador.createRelationshipTo(newNode, Neo4jConfiguration.EDGE);
		}

		return newNode;
	}

	private Node chooseAmbassador() {
		// Select a random ambassador by assigning each node a random value and picking the node with the largest value
		Node ambassador = null;
		float maxValue = Float.NEGATIVE_INFINITY;
		for (Node possibleAmbassador : GlobalGraphOperations.at(graphDatabase).getAllNodes()) {
			float randomValue = random.nextFloat();
			if (randomValue > maxValue) {
				ambassador = possibleAmbassador;
				maxValue = randomValue;
			}
		}
		return ambassador;
	}

}
