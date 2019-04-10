package science.atlarge.graphalytics.neo4j.serializer;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.FileWriter;
import java.io.IOException;

import static science.atlarge.graphalytics.neo4j.Neo4jConfiguration.ID_PROPERTY;
import static science.atlarge.graphalytics.neo4j.algorithms.bfs.BreadthFirstSearchComputation.DISTANCE;

public class BreadthFirstSearchSerializer {

    public static void serialize(
            GraphDatabaseService graphDatabase,
            String outputPath) throws IOException {
        try (FileWriter writer = new FileWriter(outputPath)) {
            try (Transaction ignored = graphDatabase.beginTx()) {
                for (Node node : graphDatabase.getAllNodes()) {
                    long id = (long) node.getProperty(ID_PROPERTY);

                    long distance = Long.MAX_VALUE;
                    if (node.hasProperty(DISTANCE)) {
                        distance = (long) node.getProperty(DISTANCE);
                    }

                    String edgeString = String.format("%d %d\n", id, distance);
                    writer.write(edgeString);
                }
            }
        }
    }


}
