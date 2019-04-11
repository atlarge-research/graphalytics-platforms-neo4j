package science.atlarge.graphalytics.neo4j.metrics;

import science.atlarge.graphalytics.execution.PlatformExecutionException;
import science.atlarge.graphalytics.execution.RunSpecification;
import science.atlarge.graphalytics.neo4j.Neo4jConfiguration;
import science.atlarge.graphalytics.neo4j.Neo4jJob;

public abstract class AbstractNeo4jJobFactory {

    protected RunSpecification runSpecification;
    protected Neo4jConfiguration platformConfig;
    protected String inputPath;
    protected String outputPath;

    public AbstractNeo4jJobFactory(RunSpecification runSpecification, Neo4jConfiguration platformConfig, String inputPath, String outputPath) {
        this.runSpecification = runSpecification;
        this.platformConfig = platformConfig;
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public abstract Neo4jJob createBfsJob() throws PlatformExecutionException;

    public abstract Neo4jJob createCdlpJob() throws PlatformExecutionException;

    public abstract Neo4jJob createLccJob() throws PlatformExecutionException;

    public abstract Neo4jJob createPrJob() throws PlatformExecutionException;

    public abstract Neo4jJob createWccJob() throws PlatformExecutionException;

    public abstract Neo4jJob createSsspJob() throws PlatformExecutionException;


}
