package mil.nga.giat.geowave.datastore.cassandra.util;

import java.util.Iterator;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.EntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.cassandra.CassandraRow;

public class CassandraEntryIteratorWrapper<T> extends
		EntryIteratorWrapper<T>
{
	private final static Logger LOGGER = Logger.getLogger(
			CassandraEntryIteratorWrapper.class);

	public CassandraEntryIteratorWrapper(
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

	public CassandraEntryIteratorWrapper(
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
		CassandraRow entry = null;
		try {
			entry = (CassandraRow) row;
		}
		catch (final ClassCastException e) {
			LOGGER.error(
					"Row is not an accumulo row entry.");
			return null;
		}
		return CassandraUtil.decodeRow(
				entry,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

}
