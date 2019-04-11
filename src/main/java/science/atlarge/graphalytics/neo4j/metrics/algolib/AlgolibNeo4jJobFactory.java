package science.atlarge.graphalytics.neo4j.metrics.algolib;

import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.neo4j.Neo4jConfiguration;
import science.atlarge.graphalytics.neo4j.Neo4jJob;
import science.atlarge.graphalytics.neo4j.metrics.AbstractNeo4jJobFactory;
import science.atlarge.graphalytics.neo4j.metrics.algolib.bfs.BreadthFirstSearchJob;
import science.atlarge.graphalytics.neo4j.metrics.algolib.cdlp.CommunityDetectionLPJob;
import science.atlarge.graphalytics.neo4j.metrics.algolib.lcc.LocalClusteringCoefficientJob;
import science.atlarge.graphalytics.neo4j.metrics.algolib.pr.PageRankJob;
import science.atlarge.graphalytics.neo4j.metrics.algolib.sssp.SingleSourceShortestPathsJob;
import science.atlarge.graphalytics.neo4j.metrics.algolib.wcc.WeaklyConnectedComponentsJob;

public class AlgolibNeo4jJobFactory extends AbstractNeo4jJobFactory {

    public AlgolibNeo4jJobFactory(
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
    public Neo4jJob createSsspJob() {
        return new SingleSourceShortestPathsJob(
                this.runSpecification,
                this.platformConfig,
                this.inputPath,
                this.outputPath
        );
    }
}
