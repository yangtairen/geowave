package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.collect.Iterators;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexDependentDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.exceptions.MismatchedIndexToAdapterMapping;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.query.BaseConstraintsQuery;
import mil.nga.giat.geowave.core.store.base.query.BaseRowIdsQuery;
import mil.nga.giat.geowave.core.store.base.query.BaseRowPrefixQuery;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallbackList;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValueImpl;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.index.writer.IndependentAdapterIndexWriter;
import mil.nga.giat.geowave.core.store.index.writer.IndexCompositeWriter;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.query.DataIdQuery;
import mil.nga.giat.geowave.core.store.query.EverythingQuery;
import mil.nga.giat.geowave.core.store.query.PrefixIdQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.query.RowIdQuery;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public abstract class BaseDataStore<R extends GeoWaveRow> implements
		DataStore
{
	private final static Logger LOGGER = Logger.getLogger(
			BaseDataStore.class);

	protected static final String ALT_INDEX_TABLE = "_GEOWAVE_ALT_INDEX";

	protected final IndexStore indexStore;
	protected final AdapterStore adapterStore;
	protected final DataStatisticsStore statisticsStore;
	protected final SecondaryIndexDataStore secondaryIndexDataStore;
	protected final AdapterIndexMappingStore indexMappingStore;
	protected final DataStoreOperations baseOperations;
	protected final DataStoreOptions baseOptions;

	public BaseDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AdapterIndexMappingStore indexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore,
			final DataStoreOperations operations,
			final DataStoreOptions options ) {
		this.indexStore = indexStore;
		this.adapterStore = adapterStore;
		this.statisticsStore = statisticsStore;
		this.indexMappingStore = indexMappingStore;
		this.secondaryIndexDataStore = secondaryIndexDataStore;

		baseOperations = operations;
		baseOptions = options;
	}

	public void store(
			final PrimaryIndex index ) {
		if (baseOptions.isPersistIndex() && !indexStore.indexExists(
				index.getId())) {
			indexStore.addIndex(
					index);
		}
	}

	protected synchronized void store(
			final DataAdapter<?> adapter ) {
		if (baseOptions.isPersistAdapter() && !adapterStore.adapterExists(
				adapter.getAdapterId())) {
			adapterStore.addAdapter(
					adapter);
		}
	}

	@Override
	public <T> IndexWriter<T> createWriter(
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex... indices )
			throws MismatchedIndexToAdapterMapping {
		store(
				adapter);

		indexMappingStore.addAdapterIndexMapping(
				new AdapterToIndexMapping(
						adapter.getAdapterId(),
						indices));

		final IndexWriter<T>[] writers = new IndexWriter[indices.length];

		int i = 0;
		for (final PrimaryIndex index : indices) {
			final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
					statisticsStore,
					secondaryIndexDataStore,
					i == 0);

			callbackManager.setPersistStats(
					baseOptions.isPersistDataStatistics());

			final List<IngestCallback<T>> callbacks = new ArrayList<IngestCallback<T>>();

			store(
					index);

			final String indexName = index.getId().getString();

			if (adapter instanceof WritableDataAdapter) {
				if (baseOptions.isUseAltIndex()) {
					addAltIndexCallback(
							callbacks,
							indexName,
							adapter,
							index.getId());
				}
			}
			callbacks.add(
					callbackManager.getIngestCallback(
							adapter,
							index));

			initOnIndexWriterCreate(
					adapter,
					index);

			final IngestCallbackList<T> callbacksList = new IngestCallbackList<T>(
					callbacks);
			writers[i] = createIndexWriter(
					adapter,
					index,
					baseOperations,
					baseOptions,
					callbacksList,
					callbacksList);

			if (adapter instanceof IndexDependentDataAdapter) {
				writers[i] = new IndependentAdapterIndexWriter<T>(
						(IndexDependentDataAdapter<T>) adapter,
						index,
						writers[i]);
			}
			i++;
		}
		return new IndexCompositeWriter(
				writers);

	}

	/*
	 * Since this general-purpose method crosses multiple adapters, the type of
	 * result cannot be assumed.
	 *
	 * (non-Javadoc)
	 *
	 * @see
	 * mil.nga.giat.geowave.core.store.DataStore#query(mil.nga.giat.geowave.
	 * core.store.query.QueryOptions,
	 * mil.nga.giat.geowave.core.store.query.Query)
	 */
	@Override
	public <T> CloseableIterator<T> query(
			final QueryOptions queryOptions,
			final Query query ) {
		final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
		// all queries will use the same instance of the dedupe filter for
		// client side filtering because the filter needs to be applied across
		// indices
		final QueryOptions sanitizedQueryOptions = (queryOptions == null) ? new QueryOptions() : queryOptions;
		final Query sanitizedQuery = (query == null) ? new EverythingQuery() : query;

		final DedupeFilter filter = new DedupeFilter();
		MemoryAdapterStore tempAdapterStore;
		try {
			tempAdapterStore = new MemoryAdapterStore(
					sanitizedQueryOptions.getAdaptersArray(
							adapterStore));

			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : sanitizedQueryOptions
					.getAdaptersWithMinimalSetOfIndices(
							tempAdapterStore,
							indexMappingStore,
							indexStore)) {
				final List<ByteArrayId> adapterIdsToQuery = new ArrayList<>();
				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {
					if (sanitizedQuery instanceof RowIdQuery) {
						sanitizedQueryOptions.setLimit(
								-1);
						results.add(
								queryRowIds(
										adapter,
										indexAdapterPair.getLeft(),
										((RowIdQuery) sanitizedQuery).getRowIds(),
										filter,
										sanitizedQueryOptions,
										tempAdapterStore));
						continue;
					}
					else if (sanitizedQuery instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) sanitizedQuery;
						if (idQuery.getAdapterId().equals(
								adapter.getAdapterId())) {
							results.add(
									getEntries(
											indexAdapterPair.getLeft(),
											idQuery.getDataIds(),
											(DataAdapter<Object>) adapterStore.getAdapter(
													idQuery.getAdapterId()),
											filter,
											sanitizedQueryOptions));
						}
						continue;
					}
					else if (sanitizedQuery instanceof PrefixIdQuery) {
						final PrefixIdQuery prefixIdQuery = (PrefixIdQuery) sanitizedQuery;
						results.add(
								queryRowPrefix(
										indexAdapterPair.getLeft(),
										prefixIdQuery.getRowPrefix(),
										sanitizedQueryOptions,
										tempAdapterStore,
										adapterIdsToQuery));
						continue;
					}
					adapterIdsToQuery.add(
							adapter.getAdapterId());
				}
				// supports querying multiple adapters in a single index
				// in one query instance (one scanner) for efficiency
				if (adapterIdsToQuery.size() > 0) {
					results.add(
							queryConstraints(
									adapterIdsToQuery,
									indexAdapterPair.getLeft(),
									sanitizedQuery,
									filter,
									sanitizedQueryOptions,
									tempAdapterStore));
				}
			}

		}
		catch (final IOException e1) {
			LOGGER.error(
					"Failed to resolve adapter or index for query",
					e1);
		}
		return new CloseableIteratorWrapper<T>(
				new Closeable() {
					@Override
					public void close()
							throws IOException {
						for (final CloseableIterator<Object> result : results) {
							result.close();
						}
					}
				},
				Iterators.concat(
						new CastIterator<T>(
								results.iterator())));
	}

	@SuppressWarnings("unchecked")
	protected CloseableIterator<Object> getEntries(
			final PrimaryIndex index,
			final List<ByteArrayId> dataIds,
			final DataAdapter<Object> adapter,
			final DedupeFilter dedupeFilter,
			final QueryOptions sanitizedQueryOptions )
			throws IOException {
		final String altIdxTableName = index.getId().getString() + ALT_INDEX_TABLE;

		MemoryAdapterStore tempAdapterStore;

		tempAdapterStore = new MemoryAdapterStore(
				new DataAdapter[] {
					adapter
				});

		if (baseOptions.isUseAltIndex() && baseOperations.indexExists(
				new ByteArrayId(
						altIdxTableName))) {
			final InsertionIds rowIds = getAltIndexInsertionIds(
					altIdxTableName,
					dataIds,
					adapter.getAdapterId());

			if (!rowIds.isEmpty()) {

				final QueryOptions options = new QueryOptions();
				options.setScanCallback(
						sanitizedQueryOptions.getScanCallback());
				options.setAuthorizations(
						sanitizedQueryOptions.getAuthorizations());
				options.setMaxResolutionSubsamplingPerDimension(
						sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension());
				options.setLimit(
						-1);

				return queryRowIds(
						adapter,
						index,
						rowIds,
						dedupeFilter,
						options,
						tempAdapterStore);
			}
		}
		else {
			return getEntryRows(
					index,
					tempAdapterStore,
					dataIds,
					adapter,
					dedupeFilter,
					sanitizedQueryOptions);
		}
		return new CloseableIterator.Empty();
	}

	@Override
	public boolean delete(
			final QueryOptions queryOptions,
			final Query query ) {
		if (((query == null) || (query instanceof EverythingQuery)) && queryOptions.isAllAdapters()) {
			try {

				indexStore.removeAll();
				adapterStore.removeAll();
				statisticsStore.removeAll();
				secondaryIndexDataStore.removeAll();
				indexMappingStore.removeAll();

				baseOperations.deleteAll();
				return true;
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to delete all tables",
						e);

			}
			return false;
		}

		final AtomicBoolean aOk = new AtomicBoolean(
				true);

		// keep a list of adapters that have been queried, to only low an
		// adapter to be queried
		// once
		final Set<ByteArrayId> queriedAdapters = new HashSet<ByteArrayId>();
		Deleter idxDeleter = null, altIdxDeleter = null;
		try {
			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : queryOptions
					.getIndicesForAdapters(
							adapterStore,
							indexMappingStore,
							indexStore)) {
				final PrimaryIndex index = indexAdapterPair.getLeft();
				if (index == null) {
					continue;
				}
				final String indexTableName = index.getId().getString();
				final String altIdxTableName = indexTableName + ALT_INDEX_TABLE;

				idxDeleter = baseOperations.createDeleter(
						index.getId(),
						queryOptions.getAuthorizations());

				altIdxDeleter = baseOptions.isUseAltIndex() && baseOperations.indexExists(
						new ByteArrayId(
								altIdxTableName))
										? baseOperations.createDeleter(
												new ByteArrayId(
														altIdxTableName),
												queryOptions.getAuthorizations())
										: null;

				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {

					final DataStoreCallbackManager callbackCache = new DataStoreCallbackManager(
							statisticsStore,
							secondaryIndexDataStore,
							queriedAdapters.add(
									adapter.getAdapterId()));

					callbackCache.setPersistStats(
							baseOptions.isPersistDataStatistics());

					if (query instanceof EverythingQuery) {
						deleteEntries(
								adapter,
								index,
								queryOptions.getAuthorizations());
						continue;
					}
					final Deleter internalIdxDeleter = idxDeleter;
					final Deleter internalAltIdxDeleter = altIdxDeleter;
					final ScanCallback<Object, R> callback = new ScanCallback<Object, R>() {
						@Override
						public void entryScanned(
								final Object entry,
								final R row ) {
							callbackCache.getDeleteCallback(
									(WritableDataAdapter<Object>) adapter,
									index).entryDeleted(
											entry,
											row);
							try {
								internalIdxDeleter.delete(
										row,
										adapter);
								if (internalAltIdxDeleter != null) {
									internalAltIdxDeleter.delete(
											row,
											adapter);
								}
							}
							catch (final Exception e) {
								LOGGER.error(
										"Failed deletion",
										e);
								aOk.set(
										false);
							}
						}
					};

					CloseableIterator<?> dataIt = null;
					queryOptions.setScanCallback(
							callback);
					final List<ByteArrayId> adapterIds = Collections.singletonList(
							adapter.getAdapterId());
					if (query instanceof RowIdQuery) {
						queryOptions.setLimit(
								-1);
						dataIt = queryRowIds(
								adapter,
								index,
								((RowIdQuery) query).getRowIds(),
								null,
								queryOptions,
								adapterStore);
					}
					else if (query instanceof DataIdQuery) {
						final DataIdQuery idQuery = (DataIdQuery) query;
						dataIt = getEntries(
								index,
								idQuery.getDataIds(),
								adapter,
								null,
								queryOptions);
					}
					else if (query instanceof PrefixIdQuery) {
						dataIt = queryRowPrefix(
								index,
								((PrefixIdQuery) query).getRowPrefix(),
								queryOptions,
								adapterStore,
								adapterIds);
					}
					else {
						dataIt = queryConstraints(
								adapterIds,
								index,
								query,
								null,
								queryOptions,
								adapterStore);
					}

					while (dataIt.hasNext()) {
						dataIt.next();
					}
					try {
						dataIt.close();
					}
					catch (final Exception ex) {
						LOGGER.warn(
								"Cannot close iterator",
								ex);
					}
					callbackCache.close();
				}
			}

			return aOk.get();
		}
		catch (final Exception e) {
			LOGGER.error(
					"Failed delete operation " + query.toString(),
					e);
			return false;
		}
		finally {
			try {
				if (idxDeleter != null) {
					idxDeleter.close();
				}
				if (altIdxDeleter != null) {
					altIdxDeleter.close();
				}
			}
			catch (final Exception e) {
				LOGGER.warn(
						"Unable to close deleter",
						e);
			}
		}

	}

	private <T> void deleteEntries(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final String... additionalAuthorizations )
			throws IOException {
		final String altIdxTableName = index.getId().getString() + ALT_INDEX_TABLE;

		try (final CloseableIterator<DataStatistics<?>> it = statisticsStore.getDataStatistics(
				adapter.getAdapterId())) {

			while (it.hasNext()) {
				final DataStatistics<?> stats = it.next();
				statisticsStore.removeStatistics(
						adapter.getAdapterId(),
						stats.getStatisticsId(),
						additionalAuthorizations);
			}
		}

		// cannot delete because authorizations are not used
		// this.indexMappingStore.remove(adapter.getAdapterId());

		baseOperations.deleteAll(
				index.getId(),
				adapter.getAdapterId(),
				additionalAuthorizations);
		if (baseOptions.isUseAltIndex() && baseOperations.indexExists(
				new ByteArrayId(
						altIdxTableName))) {
			baseOperations.deleteAll(
					new ByteArrayId(
							altIdxTableName),
					adapter.getAdapterId(),
					additionalAuthorizations);
		}
	}

	protected InsertionIds getAltIndexInsertionIds(
			final String altIdxTableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {
		// TODO: GEOWAVE-1018 - this really should be a secondary index and not
		// special cased
		return new InsertionIds();

	}

	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final DedupeFilter dedupeFilter,
			final QueryOptions queryOptions ) {
		return queryConstraints(
				Collections.singletonList(
						adapter.getAdapterId()),
				index,
				new EverythingQuery(),
				dedupeFilter,
				queryOptions,
				tempAdapterStore);
	}

	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		final BaseConstraintsQuery constraintsQuery = new BaseConstraintsQuery(
				this,
				adapterIdsToQuery,
				index,
				sanitizedQuery,
				filter,
				sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getAggregation(),
				sanitizedQueryOptions.getFieldIdsAdapterPair(),
				IndexMetaDataSet.getIndexMetadata(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DuplicateEntryCount.getDuplicateCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return constraintsQuery.query(
				baseOperations,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	protected CloseableIterator<Object> queryRowPrefix(
			final PrimaryIndex index,
			final ByteArrayId sortPrefix,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> adapterIdsToQuery ) {
		final BaseRowPrefixQuery<Object> prefixQuery = new BaseRowPrefixQuery<Object>(
				this,
				index,
				sortPrefix,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
				sanitizedQueryOptions.getLimit(),
				DifferingFieldVisibilityEntryCount.getVisibilityCounts(
						index,
						adapterIdsToQuery,
						statisticsStore,
						sanitizedQueryOptions.getAuthorizations()),
				sanitizedQueryOptions.getAuthorizations());

		return prefixQuery.query(
				baseOperations,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				tempAdapterStore);

	}

	protected CloseableIterator<Object> queryRowIds(
			final DataAdapter<Object> adapter,
			final PrimaryIndex index,
			final InsertionIds rowIds,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		// final DifferingFieldVisibilityEntryCount visibilityCounts =
		// DifferingFieldVisibilityEntryCount
		// .getVisibilityCounts(
		// index,
		// Collections.singletonList(adapter.getAdapterId()),
		// statisticsStore,
		// sanitizedQueryOptions.getAuthorizations());
		// boolean isWholeRow = (visibilityCounts == null) ||
		// visibilityCounts.isAnyEntryDifferingFieldVisiblity();
		// return baseOperations.createReader(index, adapter.getAdapterId(),
		// sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
		// sanitizedQueryOptions.getAggregation(), isWholeRow, rowIds, filter,
		// sanitizedQueryOptions.getLimit(),
		// sanitizedQueryOptions.getAuthorizations());

		final BaseRowIdsQuery<Object> q = new BaseRowIdsQuery<Object>(
				this,
				adapter,
				index,
				rowIds,
				(ScanCallback<Object, ?>) sanitizedQueryOptions.getScanCallback(),
				filter,
				sanitizedQueryOptions.getAuthorizations());

		return q.query(
				this.baseOperations,
				tempAdapterStore,
				sanitizedQueryOptions.getMaxResolutionSubsamplingPerDimension(),
				sanitizedQueryOptions.getLimit());
	}

	protected abstract <T> void addAltIndexCallback(
			List<IngestCallback<T>> callbacks,
			String indexName,
			DataAdapter<T> adapter,
			ByteArrayId primaryIndexId );

	protected <T> IndexWriter<T> createIndexWriter(
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		return new BaseIndexWriter<T>(
				adapter,
				index,
				baseOperations,
				baseOptions,
				callback,
				closable);
	}

	protected abstract <T> void initOnIndexWriterCreate(
			final DataAdapter<T> adapter,
			final PrimaryIndex index );

	/**
	 * Basic method that decodes a native row Currently overridden by Accumulo
	 * and HBase; Unification in progress
	 *
	 * Override this method if you can't pass in a GeoWaveRow!
	 */
	public <T> Object decodeRow(
			final R geowaveRow,
			final boolean wholeRowEncoding,
			final QueryFilter clientFilter,
			final DataAdapter<T> adapter,
			final AdapterStore adapterStore,
			final PrimaryIndex index,
			final ScanCallback scanCallback,
			final byte[] fieldSubsetBitmask,
			final boolean decodeRow ) {
		final ByteArrayId adapterId = new ByteArrayId(
				geowaveRow.getAdapterId());

		if ((adapter == null) && (adapterStore == null)) {
			LOGGER.error(
					"Could not decode row from iterator. Either adapter or adapter store must be non-null.");
			return null;
		}
		final IntermediaryReadEntryInfo decodePackage = new IntermediaryReadEntryInfo(
				index,
				decodeRow);

		if (!decodePackage.setOrRetrieveAdapter(
				adapter,
				adapterId,
				adapterStore)) {
			LOGGER.error(
					"Could not retrieve adapter from adapter store.");
			return null;
		}

		// Verify the adapter matches the data
		if (!decodePackage.isAdapterVerified()) {
			if (!decodePackage.verifyAdapter(
					adapterId)) {
				LOGGER.error(
						"Adapter verify failed: adapter does not match data.");
				return null;
			}
		}

		for (final GeoWaveValue value : geowaveRow.getFieldValues()) {
			byte[] byteValue = value.getValue();
			byte[] fieldMask = value.getFieldMask();

			if (fieldSubsetBitmask != null) {
				final byte[] newBitmask = BitmaskUtils.generateANDBitmask(
						fieldMask,
						fieldSubsetBitmask);
				byteValue = BitmaskUtils.constructNewValue(
						byteValue,
						fieldMask,
						newBitmask);
				if ((byteValue == null) || (byteValue.length == 0)) {
					continue;
				}
				fieldMask = newBitmask;
			}

			readValue(
					decodePackage,
					new GeoWaveValueImpl(
							fieldMask,
							value.getVisibility(),
							byteValue));
		}

		return getDecodedRow(
				geowaveRow,
				decodePackage,
				clientFilter,
				scanCallback);
	}

	/**
	 * Generic field reader - updates fieldInfoList from field input data
	 */
	protected void readValue(
			final IntermediaryReadEntryInfo decodePackage,
			final GeoWaveValue value ) {
		final List<FlattenedFieldInfo> fieldInfos = DataStoreUtils.decomposeFlattenedFields(
				value.getFieldMask(),
				value.getValue(),
				value.getVisibility(),
				-1).getFieldsRead();
		for (final FlattenedFieldInfo fieldInfo : fieldInfos) {
			final ByteArrayId fieldId = decodePackage.getDataAdapter().getFieldIdForPosition(
					decodePackage.getIndex().getIndexModel(),
					fieldInfo.getFieldPosition());
			final FieldReader<? extends CommonIndexValue> indexFieldReader = decodePackage
					.getIndex()
					.getIndexModel()
					.getReader(
							fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(
						fieldInfo.getValue());
				indexValue.setVisibility(
						value.getVisibility());
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				decodePackage.getIndexData().addValue(
						val);
			}
			else {
				final FieldReader<?> extFieldReader = decodePackage.getDataAdapter().getReader(
						fieldId);
				if (extFieldReader != null) {
					final Object objValue = extFieldReader.readField(
							fieldInfo.getValue());
					final PersistentValue<Object> val = new PersistentValue<Object>(
							fieldId,
							objValue);
					// TODO GEOWAVE-1018, do we care about visibility
					decodePackage.getExtendedData().addValue(
							val);
				}
				else {
					LOGGER.error(
							"field reader not found for data entry, the value may be ignored");
					decodePackage.getUnknownData().addValue(
							new PersistentValue<byte[]>(
									fieldId,
									fieldInfo.getValue()));
				}
			}
		}
	}

	/**
	 * build a persistence encoding object first, pass it through the client
	 * filters and if its accepted, use the data adapter to decode the
	 * persistence model into the native data type
	 */
	protected <T> Object getDecodedRow(
			final R row,
			final IntermediaryReadEntryInfo<T> decodePackage,
			final QueryFilter clientFilter,
			final ScanCallback<T, R> scanCallback ) {
		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				decodePackage.getDataAdapter().getAdapterId(),
				new ByteArrayId(
						row.getDataId()),
				new ByteArrayId(
						row.getPartitionKey()),
				new ByteArrayId(
						row.getSortKey()),
				row.getNumberOfDuplicates(),
				decodePackage.getIndexData(),
				decodePackage.getUnknownData(),
				decodePackage.getExtendedData());

		if ((clientFilter == null) || clientFilter.accept(
				decodePackage.getIndex().getIndexModel(),
				encodedRow)) {
			if (!decodePackage.isDecodeRow()) {
				return encodedRow;
			}

			final T decodedRow = decodePackage.getDataAdapter().decode(
					encodedRow,
					decodePackage.getIndex());

			if (scanCallback != null) {
				scanCallback.entryScanned(
						decodedRow,
						row);
			}

			return decodedRow;
		}

		return null;
	}
}
