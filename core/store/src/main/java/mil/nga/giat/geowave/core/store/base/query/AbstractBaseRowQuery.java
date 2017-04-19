package mil.nga.giat.geowave.core.store.base.query;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.NativeEntryIteratorWrapper;

/**
 * Represents a query operation by an Accumulo row. This abstraction is
 * re-usable for both exact row ID queries and row prefix queries.
 *
 */
abstract public class AbstractBaseRowQuery<T> extends
		BaseQuery
{
	private static final Logger LOGGER = Logger.getLogger(
			AbstractBaseRowQuery.class);
	protected final ScanCallback<T, ?> scanCallback;

	public AbstractBaseRowQuery(
			final BaseDataStore dataStore,
			final PrimaryIndex index,
			final String[] authorizations,
			final ScanCallback<T, ?> scanCallback,
			final DifferingFieldVisibilityEntryCount visibilityCounts ) {
		super(
				dataStore,
				index,
				visibilityCounts,
				authorizations);
		this.scanCallback = scanCallback;
	}

	public CloseableIterator<T> query(
			final DataStoreOperations operations,
			final double[] maxResolutionSubsamplingPerDimension,
			final AdapterStore adapterStore ) {
		return (CloseableIterator<T>) new NativeEntryIteratorWrapper(
				dataStore,
				adapterStore,
				index,
				operations.createReader(
						index,
						adapterIds,
						maxResolutionSubsamplingPerDimension,
						getAggregation(),
						getFieldSubsets(),
						useWholeRowIterator(),
						getRanges(),
						getServerFilter(),
						getScannerLimit(),
						getAdditionalAuthorizations()),
				getClientFilter(),
				scanCallback,
				!isCommonIndexAggregation());
	}

	abstract protected Integer getScannerLimit();
}
