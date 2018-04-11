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

import org.neo4j.graphdb.GraphDatabaseService;
import science.atlarge.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.neo4j.Neo4jJob;

import java.net.URL;

/**
 * Neo4j job configuration for executing the breadth-first search algorithm.
 *
 * @author Tim Hegeman
 */
public class BreadthFirstSearchJob extends Neo4jJob {

	private final BreadthFirstSearchParameters parameters;

	/**
	 * @param databasePath   path to the Neo4j database representing the graph
	 * @param propertiesFile URL of a neo4j.properties file to load from
	 * @param parameters     algorithm-specific parameters, must be of type BreadthFirstSearchParameters
	 */
	public BreadthFirstSearchJob(String databasePath, URL propertiesFile, Object parameters) {
		super(databasePath, propertiesFile);
		this.parameters = (BreadthFirstSearchParameters) parameters;
	}

	@Override
	public void runComputation(GraphDatabaseService graphDatabase, Graph graph) {
		new BreadthFirstSearchComputation(graphDatabase, parameters.getSourceVertex(), graph.isDirected()).run();
	}

}
