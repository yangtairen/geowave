package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.EntryIteratorWrapper;

public class DynamoDBEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(
			DynamoDBEntryIteratorWrapper.class);

	public DynamoDBEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter ) {
		super(
				false,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				null);
	}

	public DynamoDBEntryIteratorWrapper(
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final Iterator scannerIt,
			final QueryFilter clientFilter,
			final ScanCallback<T> scanCallback ) {
		super(
				false,
				adapterStore,
				index,
				scannerIt,
				clientFilter,
				scanCallback);
	}

	@Override
	protected T decodeRow(
			final Object row,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final boolean wholeRowEncoding ) {
		Map<String, AttributeValue> entry = null;
		try {
			entry = (Map<String, AttributeValue>) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error(
					"Row is not an accumulo row entry.");
			return null;
		}
		return DynamoDBUtils.decodeRow(
				entry,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

}
