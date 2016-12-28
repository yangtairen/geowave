package mil.nga.giat.geowave.datastore.cassandra;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.cassandra.io.sstable.CQLSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.Insert;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.MoreExecutors;

import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class CassandraWriter implements
		Writer<Insert>
{
	private static final Logger LOGGER = LoggerFactory.getLogger(
			CassandraWriter.class);
	private static final int NUM_INSERTS = 1000;
	private static final int INGEST_THREAD_POOL = 16;
	private ExecutorService executor = MoreExecutors.getExitingExecutorService(
			(ThreadPoolExecutor) Executors.newFixedThreadPool(
					INGEST_THREAD_POOL));


	private final List<Insert> batchedInserts = new ArrayList<>();
	private PreparedStatement preparedInsert;
	private final String tableName;
	private final CassandraOperations operations;

	public CassandraWriter(
			final String tableName,
			final CassandraOperations operations ) {
		this.tableName = tableName;
		this.operations = operations;
	}

	@Override
	public void close()
			throws IOException {
		flush();
	}

	@Override
	public void write(
			final Iterable<Insert> inserts ) {
		for (final Insert item : inserts) {
			write(
					item);
		}
	}

	@Override
	public void write(
			final Insert insert ) {
		synchronized (batchedInserts) {
			if (batchedInserts.size() >= NUM_INSERTS) {
				do {
					writeBatch(
							true);
				}
				while (batchedInserts.size() >= NUM_INSERTS);
			}
			else {
				batchedInserts.add(
						insert);
			}
		}
	}
	
	private Insert getInsert(){
		
	}

	private synchronized void writeBatch(
			final boolean async ) {
		if (preparedInsert == null){
			preparedInsert = operations.getSession().prepare(getInsert());
		}
		operations.getSession().executeAsync(preparedInsert);
		final List<WriteRequest> batch;

		if (batchedItems.size() <= NUM_ITEMS) {
			batch = batchedItems;
		}
		else {
			batch = batchedItems.subList(
					0,
					NUM_ITEMS);
		}
		final Map<String, List<WriteRequest>> writes = new HashMap<>();
		writes.put(
				tableName,
				new ArrayList<>(
						batch));
		// if (async) {
		// final Future<BatchWriteItemResult> response =
		// client.batchWriteItemAsync(
		// new BatchWriteItemRequest(
		// writes));
		//
		// DynamoDBClientPool.DYNAMO_RETRY_POOL.execute(
		// new Runnable() {
		// @Override
		// public void run() {
		// try {
		// final Map<String, List<WriteRequest>> map =
		// response.get().getUnprocessedItems();
		// retry(
		// map);
		// }
		// catch (InterruptedException | ExecutionException e) {
		// LOGGER.warn(
		// "Unable to get response from Async Write",
		// e);
		// }
		// }
		// });
		// }
		// else {
		final BatchWriteItemResult response = client.batchWriteItem(
				new BatchWriteItemRequest(
						writes));
		retry(
				response.getUnprocessedItems());
		// }
		batch.clear();
	}

	private void retry(
			final Map<String, List<WriteRequest>> map ) {
		for (final Entry<String, List<WriteRequest>> requests : map.entrySet()) {
			for (final WriteRequest r : requests.getValue()) {
				if (r.getPutRequest() != null) {
					client.putItem(
							requests.getKey(),
							r.getPutRequest().getItem());
				}
			}
		}
	}

	@Override
	public void flush() {
		synchronized (batchedItems) {
			do {
				writeBatch(
						false);
			}
			while (!batchedItems.isEmpty());
		}
	}

	//callback class
	private static class IngestCallback implements FutureCallback<ResultSet>{

	    @Override
	    public void onSuccess(ResultSet result) {
	        //placeholder: put any logging or on success logic here.
	    }

	    @Override
	    public void onFailure(Throwable t) {
	        //go ahead and wrap in a runtime exception for this case, but you can do logging or start counting errors.
	        throw new RuntimeException(t);
	    }
	}
}
