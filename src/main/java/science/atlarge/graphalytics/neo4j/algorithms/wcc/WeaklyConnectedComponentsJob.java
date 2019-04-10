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
package science.atlarge.graphalytics.neo4j.algorithms.wcc;

import org.neo4j.graphdb.GraphDatabaseService;
import science.atlarge.graphalytics.domain.graph.Graph;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.neo4j.Neo4jConfiguration;
import science.atlarge.graphalytics.neo4j.Neo4jJob;

/**
 * Neo4j job configuration for executing the connected components algorithm.
 *
 * @author Tim Hegeman
 */
public class WeaklyConnectedComponentsJob extends Neo4jJob {

	public WeaklyConnectedComponentsJob(RunSpecification runSpecification, Neo4jConfiguration platformConfig,
                                        String inputPath, String outputPath) {
        super(runSpecification, platformConfig, inputPath, outputPath);
    }

	@Override
	public void runComputation(GraphDatabaseService graphDatabase, Graph graph) {
		new WeaklyConnectedComponentsComputation(
				graphDatabase
		).run();
	}

}
