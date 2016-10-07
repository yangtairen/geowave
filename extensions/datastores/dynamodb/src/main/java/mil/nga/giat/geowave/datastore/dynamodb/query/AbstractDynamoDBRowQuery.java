package mil.nga.giat.geowave.datastore.dynamodb.query;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;
import mil.nga.giat.geowave.datastore.dynamodb.util.DynamoDBEntryIteratorWrapper;

/**
 * Represents a query operation by an DynamoDB row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 *
 */
abstract public class AbstractDynamoDBRowQuery<T> extends
		DynamoDBQuery
{
	private static final Logger LOGGER = Logger.getLogger(
			AbstractDynamoDBRowQuery.class);
	protected final ScanCallback<T> scanCallback;

	public AbstractDynamoDBRowQuery(
			final DynamoDBOperations dynamodbOperations,
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts ) {
		super(
				dynamodbOperations,
				index,
				visibilityCounts,
				authorizations);
		this.scanCallback = scanCallback;
	}

	public CloseableIterator<T> query(
			final double[] maxResolutionSubsamplingPerDimension,
			final AdapterStore adapterStore ) {
		final Iterator<Map<String, AttributeValue>> results = getResults(
				maxResolutionSubsamplingPerDimension,
				getScannerLimit());
		return new CloseableIterator.Wrapper<T>(
				new DynamoDBEntryIteratorWrapper(
						adapterStore,
						index,
						results,
						null,
						this.scanCallback));
	}

	abstract protected Integer getScannerLimit();
}
