package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

import mil.nga.giat.geowave.core.store.DataStoreOperations;

public class DynamoDBOperations implements
		DataStoreOperations
{
	private final Logger LOGGER = LoggerFactory.getLogger(
			DynamoDBOperations.class);
	private final AmazonDynamoDBAsyncClient client;
	private final String gwNamespace;
	private final DynamoDBOptions options;

	public DynamoDBOperations(
			final DynamoDBOptions options ) {
		this.options = options;
		client = DynamoDBClientPool.getInstance().getClient(
				options);
		gwNamespace = options.getGeowaveNamespace();

	}

	public DynamoDBOptions getOptions() {
		return options;
	}

	public AmazonDynamoDBAsyncClient getClient() {
		return client;
	}

	public String getQualifiedTableName(
			final String tableName ) {
		return gwNamespace == null ? tableName : gwNamespace + "_" + tableName;
	}

	@Override
	public boolean tableExists(
			final String tableName )
			throws IOException {
		try {
			return TableStatus.ACTIVE.name().equals(
					client
							.describeTable(
									getQualifiedTableName(
											tableName))
							.getTable()
							.getTableStatus());
		}
		catch (final AmazonDynamoDBException e) {
			LOGGER.info(
					"Unable to check existence of table",
					e);
		}
		return false;
	}

	@Override
	public void deleteAll()
			throws Exception {
		final ListTablesResult tables = client.listTables();
		for (final String tableName : tables.getTableNames()) {
			if ((gwNamespace == null) || tableName.startsWith(
					gwNamespace)) {
				client.deleteTable(
						new DeleteTableRequest(
								tableName));
			}
		}
	}

	@Override
	public String getTableNameSpace() {
		return gwNamespace;
	}

}
