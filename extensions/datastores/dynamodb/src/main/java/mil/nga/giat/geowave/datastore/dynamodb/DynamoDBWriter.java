package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;

import mil.nga.giat.geowave.core.store.base.Writer;

public class DynamoDBWriter implements
		Writer<Item>
{
	private static final int NUM_ITEMS = 25;
	private final AmazonDynamoDBAsyncClient client = new AmazonDynamoDBAsyncClient().withEndpoint(
			"http://localhost:8000");

	private List<Item> batchedItems;

	@Override
	public void close()
			throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void write(
			final Iterable<Item> items ) {
		for (Item item : items){
			write(item);
		}
	}

	@Override
	public void write(
			final Item item ) {
		if (batchedItems.size() >= NUM_ITEMS){
			client.batchWriteItemAsync(new BatchWriteItemRequest().withRequestItems(requestItems));
		}
	}

	@Override
	public void flush() {
		// TODO Auto-generated method stub

	}

}
