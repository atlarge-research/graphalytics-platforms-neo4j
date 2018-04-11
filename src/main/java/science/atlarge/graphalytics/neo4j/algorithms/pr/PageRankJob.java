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
package science.atlarge.graphalytics.neo4j.algorithms.pr;

import org.neo4j.graphdb.GraphDatabaseService;
import science.atlarge.graphalytics.domain.algorithms.PageRankParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.neo4j.Neo4jJob;

import java.net.URL;

/**
 * Neo4j job configuration for calculating the PageRank values of nodes in a graph.
 *
 * @author Tim Hegeman
 */
public class PageRankJob extends Neo4jJob {

	private final PageRankParameters parameters;

	/**
	 * @param databasePath   the path of the pre-loaded graph database
	 * @param propertiesFile a Neo4j properties file
	 */
	public PageRankJob(String databasePath, URL propertiesFile, Object parameters) {
		super(databasePath, propertiesFile);
		this.parameters = (PageRankParameters)parameters;
	}

	@Override
	public void runComputation(GraphDatabaseService graphDatabase, Graph graph) {
		new PageRankComputation(graphDatabase, parameters.getNumberOfIterations(), parameters.getDampingFactor(),
				(int)graph.getNumberOfVertices()).run();
	}

}