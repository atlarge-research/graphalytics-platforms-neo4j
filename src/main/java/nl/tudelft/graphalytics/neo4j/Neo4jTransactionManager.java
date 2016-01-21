package nl.tudelft.graphalytics.neo4j;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

/**
 * Utility class for managing transactions in Neo4j. Tracks the number of operations in a transaction to commit
 * whenever a threshold is reached.
 *
 * @author Tim Hegeman
 */
public class Neo4jTransactionManager implements AutoCloseable {

	public static final long DEFAULT_MAXIMUM_OPERATIONS_PER_TRANSACTION = 4095;

	private final GraphDatabaseService db;
	private final long maximumOperationsPerTransaction;
	private Transaction transaction;
	private long operationsInTransaction;

	public Neo4jTransactionManager(GraphDatabaseService db) {
		this(db, DEFAULT_MAXIMUM_OPERATIONS_PER_TRANSACTION);
	}

	public Neo4jTransactionManager(GraphDatabaseService db, long maximumOperationsPerTransaction) {
		this.db = db;
		this.maximumOperationsPerTransaction = maximumOperationsPerTransaction;
		this.transaction = db.beginTx();
		this.operationsInTransaction = 0;
	}

	public void incrementOperations() {
		operationsInTransaction++;
		if (operationsInTransaction == maximumOperationsPerTransaction) {
			transaction.success();
			transaction.close();
			transaction = db.beginTx();
			operationsInTransaction = 0;
		}
	}

	public void forceCommit() {
		transaction.success();
		transaction.close();
		transaction = db.beginTx();
		operationsInTransaction = 0;
	}

	@Override
	public void close() {
		transaction.success();
		transaction.close();
	}
}
