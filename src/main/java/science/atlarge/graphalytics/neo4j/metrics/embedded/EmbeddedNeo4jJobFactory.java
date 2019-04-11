package science.atlarge.graphalytics.neo4j.metrics.embedded;

import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.neo4j.Neo4jConfiguration;
import science.atlarge.graphalytics.neo4j.Neo4jJob;
import science.atlarge.graphalytics.neo4j.metrics.AbstractNeo4jJobFactory;
import science.atlarge.graphalytics.neo4j.metrics.embedded.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.neo4j.metrics.embedded.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.neo4j.metrics.embedded.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.neo4j.metrics.embedded.pr.PageRankJob;
import science.atlarge.graphalytics.neo4j.metrics.embedded.wcc.WeaklyConnectedComponentsJob;

public class EmbeddedNeo4jJobFactory extends AbstractNeo4jJobFactory {

    public EmbeddedNeo4jJobFactory(
            RunSpecification runSpecification,
            Neo4jConfiguration platformConfig,
            String inputPath,
            String outputPath
    ) {
        super(runSpecification, platformConfig, inputPath, outputPath);
    }

    @Override
    public Neo4jJob createBfsJob() {
        return new BreadthFirstSearchJob(
                this.runSpecification,
                this.platformConfig,
                this.inputPath,
                this.outputPath
        );
    }

    @Override
    public Neo4jJob createCdlpJob() {
        return new CommunityDetectionLPJob(
                this.runSpecification,
                this.platformConfig,
                this.inputPath,
                this.outputPath
        );
    }

    @Override
    public Neo4jJob createLccJob() {
        return new LocalClusteringCoefficientJob(
                this.runSpecification,
                this.platformConfig,
                this.inputPath,
                this.outputPath
        );
    }

    @Override
    public Neo4jJob createPrJob() {
        return new PageRankJob(
                this.runSpecification,
                this.platformConfig,
                this.inputPath,
                this.outputPath
        );
    }

    @Override
    public Neo4jJob createWccJob() {
        return new WeaklyConnectedComponentsJob(
                this.runSpecification,
                this.platformConfig,
                this.inputPath,
                this.outputPath
        );
    }

    @Override
    public Neo4jJob createSsspJob() throws PlatformExecutionException {
        throw new PlatformExecutionException("Algorithm SSSP not supported");
    }
}
