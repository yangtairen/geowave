package mil.nga.giat.geowave.core.store.base;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.MultiDimensionalCoordinateRangesArray;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIterator.Wrapper;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.query.ConstraintsQuery;
import mil.nga.giat.geowave.core.store.query.CoordinateRangeQueryFilter;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;
import mil.nga.giat.geowave.core.store.query.aggregate.CommonIndexAggregation;

/**
 * This class represents basic numeric contraints applied to a datastore query
 *
 */
class BaseConstraintsQuery extends
		BaseFilteredIndexQuery
{

	private final static Logger LOGGER = Logger.getLogger(
			BaseConstraintsQuery.class);
	protected final ConstraintsQuery base;
	private boolean queryFiltersEnabled;

	public BaseConstraintsQuery(
			final BaseDataStore dataStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final Query query,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		this(
				dataStore,
				adapterIds,
				index,
				query != null ? query.getIndexConstraints(
						index.getIndexStrategy()) : null,
				query != null ? query.createFilters(
						index.getIndexModel()) : null,
				clientDedupeFilter,
				scanCallback,
				aggregation,
				fieldIdsAdapterPair,
				indexMetaData,
				duplicateCounts,
				visibilityCounts,
				authorizations);
	}

	public BaseConstraintsQuery(
			final BaseDataStore dataStore,
			final List<ByteArrayId> adapterIds,
			final PrimaryIndex index,
			final List<MultiDimensionalNumericData> constraints,
			final List<QueryFilter> queryFilters,
			final DedupeFilter clientDedupeFilter,
			final ScanCallback<?, ?> scanCallback,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldIdsAdapterPair,
			final IndexMetaData[] indexMetaData,
			final DuplicateEntryCount duplicateCounts,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {

		super(
				dataStore,
				adapterIds,
				index,
				scanCallback,
				fieldIdsAdapterPair,
				visibilityCounts,
				authorizations);

		base = new ConstraintsQuery(
				constraints,
				aggregation,
				indexMetaData,
				index,
				queryFilters,
				clientDedupeFilter,
				duplicateCounts,
				this);

		queryFiltersEnabled = true;
	}

	@Override
	public DistributableQueryFilter getServerFilter(
			DataStoreOptions options ) {
		//TODO GEOWAVE-1018 is options necessary?  is this correct?
		if (base.distributableFilters == null && base.distributableFilters.isEmpty()){
			return null;
		}
		else if (base.distributableFilters.size() > 1) {
			return new DistributableFilterList(
					base.distributableFilters);
		}
		else {
			return base.distributableFilters.get(0);
		}
	}

	@Override
	protected QueryRanges getRanges() {
		return base.getRanges();
	}

	public boolean isQueryFiltersEnabled() {
		return queryFiltersEnabled;
	}

	public void setQueryFiltersEnabled(
			final boolean queryFiltersEnabled ) {
		this.queryFiltersEnabled = queryFiltersEnabled;
	}

	@Override
	public CloseableIterator<Object> query(
			final DataStoreOperations datastoreOperations,
			final DataStoreOptions options,
			final AdapterStore adapterStore,
			final double[] maxResolutionSubsamplingPerDimension,
			final Integer limit ) {
		// Aggregate client-side
		if ((getAggregation() != null) && ((options == null) || !options.isServerSideLibraryEnabled())) {
			final CloseableIterator<Object> it = super.query(
					datastoreOperations,
					options,
					adapterStore,
					maxResolutionSubsamplingPerDimension,
					limit);

			if ((it != null) && it.hasNext()) {
				final Aggregation aggregationFunction = base.aggregation.getRight();
				synchronized (aggregationFunction) {
					aggregationFunction.clearResult();
					while (it.hasNext()) {
						final Object input = it.next();
						if (input != null) {
							aggregationFunction.aggregate(
									input);
						}
					}
					try {
						it.close();
					}
					catch (final IOException e) {
						LOGGER.warn(
								"Unable to close datastore reader",
								e);
					}

					return new Wrapper(
							Iterators.singletonIterator(
									aggregationFunction.getResult()));
				}
			}

			return new CloseableIterator.Empty();
		}
		// TODO: GEOWAVE-1018 what about merging results in the case of a
		// server-side aggregation?
		return super.query(
				datastoreOperations,
				options,
				adapterStore,
				maxResolutionSubsamplingPerDimension,
				limit);
	}

	@Override
	protected List<QueryFilter> getClientFiltersList(
			final DataStoreOptions options ) {

		// Since we have custom filters enabled, this list should only return
		// the client filters
		if ((options != null) && options.isServerSideLibraryEnabled()) {
			return clientFilters;
		}
		// add a index filter to the front of the list if there isn't already a
		// filter
		if (base.distributableFilters.isEmpty()
				|| ((base.distributableFilters.size() == 1) && (base.distributableFilters.get(
						0) instanceof DedupeFilter))) {
			final List<MultiDimensionalCoordinateRangesArray> coords = base.getCoordinateRanges();
			if (!coords.isEmpty()) {
				clientFilters.add(
						0,
						new CoordinateRangeQueryFilter(
								index.getIndexStrategy(),
								coords.toArray(
										new MultiDimensionalCoordinateRangesArray[] {})));
			}
		}
		else {
			// Without custom filters, we need all the filters on the client
			// side
			for (final QueryFilter distributable : base.distributableFilters) {
				if (!clientFilters.contains(
						distributable)) {
					clientFilters.add(
							distributable);
				}
			}
		}
		return clientFilters;
	}

	@Override
	protected boolean isCommonIndexAggregation() {
		return base.isAggregation() && (base.aggregation.getRight() instanceof CommonIndexAggregation);
	}

	@Override
	protected Pair<DataAdapter<?>, Aggregation<?, ?, ?>> getAggregation() {
		return base.aggregation;
	}
}
