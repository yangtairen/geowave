package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

import mil.nga.giat.geowave.core.store.DataStoreOperations;

public class DynamoDBOperations implements
		DataStoreOperations
{
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

	@Override
	public boolean tableExists(
			final String tableName )
			throws IOException {
		return TableStatus.ACTIVE.name().equals(
				client.describeTable(
						tableName).getTable().getTableStatus());
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
