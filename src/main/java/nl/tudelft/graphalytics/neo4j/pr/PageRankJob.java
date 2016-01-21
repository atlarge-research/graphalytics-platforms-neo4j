package nl.tudelft.graphalytics.neo4j.pr;

import nl.tudelft.graphalytics.domain.Graph;
import nl.tudelft.graphalytics.domain.algorithms.PageRankParameters;
import nl.tudelft.graphalytics.neo4j.Neo4jJob;
import org.neo4j.graphdb.GraphDatabaseService;

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
