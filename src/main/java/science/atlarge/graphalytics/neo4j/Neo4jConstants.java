package science.atlarge.graphalytics.neo4j;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

/**
 * Class containing variables used to identify nodes, edges, and labels.
 *
 * @author Bálint Hegyi
 */
public final class Neo4jConstants {

    //
    // General graph constants
    //

    public static final RelationshipType EDGE = RelationshipType.withName("EDGE");
    public static final String ID_PROPERTY = "VID";
    public static final String WEIGHT_PROPERTY = "WEIGHT";

    //
    // Metric properties
    //

    /**
     * BFS result property
     */
    public static final String DISTANCE = "DISTANCE";
    /**
     * LCC result property
     */
    public static final String LCC = "LCC";
    /**
     * PageRank result property
     */
    public static final String PAGERANK = "PAGERANK";
    /**
     * WCC result property
     */
    public static final String COMPONENT = "COMPONENT";
    /**
     * SSSP result property
     */
    public static final String SSSP = "SSSP";
    /**
     * CDLP result property
     */
    public static final String LABEL = "LABEL";

    public enum VertexLabelEnum implements Label {
        Vertex
    }
}
