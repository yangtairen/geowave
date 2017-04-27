package mil.nga.giat.geowave.core.store.base.query;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.DataStoreQuery;
import mil.nga.giat.geowave.core.store.base.Reader;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * This class is used internally to perform query operations against an Accumulo
 * data store. The query is defined by the set of parameters passed into the
 * constructor.
 */
abstract public class BaseQuery extends
		DataStoreQuery
{
	private final static Logger LOGGER = Logger.getLogger(
			BaseQuery.class);

	public BaseQuery(
			final BaseDataStore dataStore,
			final PrimaryIndex index,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		this(
				dataStore,
				null,
				index,
				null,
				visibilityCounts,
				authorizations);
	}

	public BaseQuery(
			final BaseDataStore dataStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String... authorizations ) {
		super(
				dataStore,
				adapterIds,
				index,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);
	}

	protected Reader getReader(
			final DataStoreOperations operations,
			final DataStoreOptions options,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		return operations.createReader(
				index,
				adapterIds,
				maxResolutionSubsamplingPerDimension,
				getAggregation(),
				getFieldSubsets(),
				useWholeRowIterator(),
				getRanges(),
				getServerFilter(
						options),
				limit,
				getAdditionalAuthorizations());
	}
}
