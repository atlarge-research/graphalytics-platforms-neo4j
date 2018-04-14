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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import science.atlarge.graphalytics.domain.algorithms.Algorithm;
import science.atlarge.graphalytics.domain.graph.FormattedGraph;
import science.atlarge.graphalytics.domain.graph.LoadedGraph;
import science.atlarge.graphalytics.execution.Platform;
import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.neo4j.algolib.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.neo4j.algorithms.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.neo4j.algorithms.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.neo4j.algorithms.ffm.ForestFireModelJob;
import science.atlarge.graphalytics.neo4j.algorithms.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.neo4j.algorithms.wcc.WeaklyConnectedComponentsJob;
import science.atlarge.graphalytics.report.result.BenchmarkMetrics;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;

/**
 * Entry point of the Graphalytics benchmark for Neo4j. Provides the platform
 * API required by the Graphalytics core to perform operations such as uploading
 * graphs and executing specific algorithms on specific graphs.
 *
 * @author Tim Hegeman
 */
public class Neo4jPlatform implements Platform {

	private static final Logger LOG = LogManager.getLogger();

	/**
	 * Property key for the directory in which to store Neo4j databases.
	 */
	public static final String DB_PATH_KEY = "neo4j.db.path";
	/**
	 * Default value for the directory in which to store Neo4j databases.
	 */
	public static final String DB_PATH = "neo4j-data";

	private static final String PROPERTIES_FILENAME = "neo4j.properties";
	public static final String PROPERTIES_PATH = "/" + PROPERTIES_FILENAME;

	private static final int INDEX_WAIT_TIMEOUT_SECONDS = 10;
	private static final int TRANSACTION_SIZE = 1000;

	private String dbPath;

	public Neo4jPlatform() {
		loadConfiguration();
	}

	private void loadConfiguration() {
		// Load Neo4j-specific configuration
		Configuration neo4jConfig;
		try {
			neo4jConfig = new PropertiesConfiguration(PROPERTIES_FILENAME);
		} catch (ConfigurationException e) {
			// Fall-back to an empty properties file
			LOG.info("Could not find or load \"{}\"", PROPERTIES_FILENAME);
			neo4jConfig = new PropertiesConfiguration();
		}
		dbPath = neo4jConfig.getString(DB_PATH_KEY, DB_PATH);
	}

	private Neo4jJob createJob(String databasePath, Algorithm algorithm, Object parameters)
			throws PlatformExecutionException {
		URL properties = getClass().getResource(PROPERTIES_PATH);
		switch (algorithm) {
			case BFS:
				return new BreadthFirstSearchJob(databasePath, properties, parameters);
			case CDLP:
				return new CommunityDetectionLPJob(databasePath, properties, parameters);
			case WCC:
				return new WeaklyConnectedComponentsJob(databasePath, properties);
			case FFM:
				return new ForestFireModelJob(databasePath, properties, parameters);
			case LCC:
				return new LocalClusteringCoefficientJob(databasePath, properties);
			case SSSP:
				return new SingleSourceShortestPathsJob(databasePath, properties, parameters);
			default:
				throw new PlatformExecutionException("Algorithm \"" + algorithm + "\" not supported");
		}
	}

	private void copyDatabase(String sourcePath, String destinationPath) throws PlatformExecutionException {
		try {
			FileUtils.copyDirectory(Paths.get(sourcePath).toFile(), Paths.get(destinationPath).toFile());
		} catch (IOException ex) {
			throw new PlatformExecutionException("Unable to create a temporary copy of the graph database:", ex);
		}
	}

	private void deleteDatabase(String databasePath) throws PlatformExecutionException {
		try {
			FileUtils.deleteDirectory(Paths.get(databasePath).toFile());
		} catch (IOException e) {
			throw new PlatformExecutionException("Unable to clean up the graph database:", e);
		}
	}

	@Override
	public void verifySetup() throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public LoadedGraph loadGraph(FormattedGraph formattedGraph) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void prepare(RunSpecification runSpecification) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void startup(RunSpecification runSpecification) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void run(RunSpecification runSpecification) throws PlatformExecutionException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BenchmarkMetrics finalize(RunSpecification runSpecification) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void terminate(RunSpecification runSpecification) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public void deleteGraph(LoadedGraph loadedGraph) throws Exception {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getPlatformName() {
		throw new UnsupportedOperationException();
	}

}
