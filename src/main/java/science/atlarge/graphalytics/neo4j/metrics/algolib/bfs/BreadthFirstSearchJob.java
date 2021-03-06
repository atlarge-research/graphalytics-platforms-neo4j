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
package science.atlarge.graphalytics.neo4j.metrics.algolib.bfs;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import science.atlarge.graphalytics.domain.algorithms.BreadthFirstSearchParameters;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.neo4j.Neo4jConfiguration;
import science.atlarge.graphalytics.neo4j.Neo4jConstants;
import science.atlarge.graphalytics.neo4j.Neo4jJob;
import science.atlarge.graphalytics.neo4j.ProcTimeLog;
import science.atlarge.graphalytics.neo4j.metrics.OutputSerializer;

import java.io.IOException;

/**
 * Neo4j job configuration for executing the breadth-first search algorithm.
 *
 * @author Tim Hegeman
 */
public class BreadthFirstSearchJob extends Neo4jJob {

	private final BreadthFirstSearchParameters parameters;

	public BreadthFirstSearchJob(RunSpecification runSpecification, Neo4jConfiguration platformConfig,
								 String inputPath, String outputPath) {
		super(runSpecification, platformConfig, inputPath, outputPath);
		this.parameters = (BreadthFirstSearchParameters)runSpecification
				.getBenchmarkRun()
				.getAlgorithmParameters();
	}

	@Override
	public void compute(GraphDatabaseService graphDatabase, Graph graph) throws KernelException {
		ProcTimeLog.start();
		BreadthFirstSearchComputation computation = new BreadthFirstSearchComputation(
				graphDatabase,
				parameters.getSourceVertex(),
				graph.isDirected()
		);
		computation.run();
		ProcTimeLog.end();
	}

	@Override
	protected void serialize(GraphDatabaseService graphDatabase, String outputPath) throws IOException {
		OutputSerializer<Long> serializer = new OutputSerializer<>(
				Neo4jConstants.DISTANCE,
				Long.MAX_VALUE
		);
		serializer.serialize(
				graphDatabase,
				outputPath
		);
	}
}
