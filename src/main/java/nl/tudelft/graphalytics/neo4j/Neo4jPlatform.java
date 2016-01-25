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
package nl.tudelft.graphalytics.neo4j;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import nl.tudelft.graphalytics.Platform;
import nl.tudelft.graphalytics.PlatformExecutionException;
import nl.tudelft.graphalytics.domain.*;
import nl.tudelft.graphalytics.neo4j.algorithms.bfs.BreadthFirstSearchJob;
import nl.tudelft.graphalytics.neo4j.algorithms.cdlp.CommunityDetectionLPJob;
import nl.tudelft.graphalytics.neo4j.algorithms.wcc.WeaklyConnectedComponentsJob;
import nl.tudelft.graphalytics.neo4j.algorithms.ffm.ForestFireModelJob;
import nl.tudelft.graphalytics.neo4j.algorithms.lcc.LocalClusteringCoefficientJob;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.EDGE;
import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static nl.tudelft.graphalytics.neo4j.Neo4jConfiguration.VertexLabelEnum.Vertex;

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

	@Override
	public void uploadGraph(Graph graph) throws Exception {
		LOG.info("Importing graph \"{}\" into a Neo4j database", graph.getName());

		String databasePath = Paths.get(dbPath, graph.getName()).toString();

		InputStream propertiesStream = getClass().getResourceAsStream(PROPERTIES_PATH);
		Map<String, String> properties = MapUtil.load(propertiesStream);
		BatchInserter inserter = BatchInserters.inserter(databasePath, properties);

		LOG.debug("- Inserting vertices");

		Long2LongMap vertexIdMap = new Long2LongOpenHashMap((int)graph.getNumberOfVertices());
		try (BufferedReader vertexData = new BufferedReader(new FileReader(graph.getVertexFilePath()))) {
			Map<String, Object> propertiesCache = new HashMap<>(1, 1.0f);
			for (String vertexLine = vertexData.readLine(); vertexLine != null; vertexLine = vertexData.readLine()) {
				if (vertexLine.isEmpty()) {
					continue;
				}

				long vertexId = Long.parseLong(vertexLine);
				propertiesCache.put(ID_PROPERTY, vertexId);
				long internalVertexId = inserter.createNode(propertiesCache, (Label)Vertex);
				vertexIdMap.put(vertexId, internalVertexId);
			}
		}

		LOG.debug("- Inserting edges");

		try (BufferedReader edgeData = new BufferedReader(new FileReader(graph.getEdgeFilePath()))) {
			for (String edgeLine = edgeData.readLine(); edgeLine != null; edgeLine = edgeData.readLine()) {
				if (edgeLine.isEmpty()) {
					continue;
				}

				String[] edgeLineChunks = edgeLine.split(" ");
				if (edgeLineChunks.length != 2) {
					throw new IOException("Invalid data found in edge list: \"" + edgeLine + "\"");
				}

				inserter.createRelationship(vertexIdMap.get(Long.parseLong(edgeLineChunks[0])),
						vertexIdMap.get(Long.parseLong(edgeLineChunks[1])), EDGE, null);
			}
		}

		inserter.createDeferredSchemaIndex(Vertex).on(ID_PROPERTY).create();

		inserter.shutdown();

		LOG.debug("- Graph \"{}\" imported successfully", graph.getName());
	}

	@Override
	public PlatformBenchmarkResult executeAlgorithmOnGraph(Benchmark benchmark) throws PlatformExecutionException {
		Algorithm algorithm = benchmark.getAlgorithm();
		Graph graph = benchmark.getGraph();
		Object parameters = benchmark.getAlgorithmParameters();

		LOG.info("Executing algorithm \"{}\" on graph \"{}\"", algorithm.getName(), graph.getName());

		// Create a copy of the database that is used to store the algorithm results
		LOG.debug("- Creating working copy of graph database");
		String graphDbPath = Paths.get(dbPath, graph.getName()).toString();
		String graphDbCopyPath = Paths.get(dbPath, graph.getName() + "-" + algorithm).toString();
		copyDatabase(graphDbPath, graphDbCopyPath);

		// Execute the algorithm
		try {
			LOG.debug("- Starting Neo4j job");
			Neo4jJob job = createJob(graphDbCopyPath, algorithm, parameters);
			job.run(graph);
		} finally {
			// Clean up the database copy
			deleteDatabase(graphDbCopyPath);
		}

		LOG.debug("- Successfully completed algorithm");

		return new PlatformBenchmarkResult(NestedConfiguration.empty());
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
	public void deleteGraph(String graphName) {
		LOG.info("Cleaning up graph \"{}\"", graphName);

		try {
			deleteDatabase(Paths.get(dbPath, graphName).toString());
		} catch (PlatformExecutionException e) {
			LOG.error("Failed to clean up the graph database at \"" + Paths.get(dbPath, graphName).toString() + "\":", e);
		}
	}

	@Override
	public String getName() {
		return "neo4j";
	}

	@Override
	public NestedConfiguration getPlatformConfiguration() {
		try {
			Configuration configuration = new PropertiesConfiguration(PROPERTIES_FILENAME);
			return NestedConfiguration.fromExternalConfiguration(configuration, PROPERTIES_FILENAME);
		} catch (ConfigurationException ex) {
			return NestedConfiguration.empty();
		}
	}

}