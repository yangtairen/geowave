package mil.nga.giat.geowave.core.store.memory;

import java.util.List;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.Deleter;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class MemoryDataStore extends
		BaseDataStore<GeoWaveRowImpl>
{
	private final static Logger LOGGER = Logger.getLogger(
			MemoryDataStore.class);

	public MemoryDataStore() {
		super(
				new MemoryIndexStore(),
				new MemoryAdapterStore(),
				new MemoryDataStatisticsStore(),
				new MemoryAdapterIndexMappingStore(),
				new MemorySecondaryIndexDataStore(),
				new MemoryDataStoreOperations(),
				new BaseDataStoreOptions());
	}

	public MemoryDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final AdapterIndexMappingStore adapterIndexMappingStore,
			final SecondaryIndexDataStore secondaryIndexDataStore ) {
		super(
				indexStore,
				adapterStore,
				statsStore,
				adapterIndexMappingStore,
				secondaryIndexDataStore,
				new MemoryDataStoreOperations(),
				new BaseDataStoreOptions());
	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {}

	@Override
	protected <T> void initOnIndexWriterCreate(
			final DataAdapter<T> adapter,
			final PrimaryIndex index ) {}

	// @Override
	// public <T> IndexWriter createWriter(
	// final DataAdapter<T> adapter,
	// final PrimaryIndex... indices )
	// throws MismatchedIndexToAdapterMapping {
	// adapterStore.addAdapter(
	// adapter);
	//
	// adapterIndexMappingStore.addAdapterIndexMapping(
	// new AdapterToIndexMapping(
	// adapter.getAdapterId(),
	// indices));
	// final IndexWriter<T>[] writers = new IndexWriter[indices.length];
	// int i = 0;
	// for (final PrimaryIndex index : indices) {
	// indexStore.addIndex(
	// index);
	// writers[i] = new MyIndexWriter<T>(
	// DataStoreUtils.UNCONSTRAINED_VISIBILITY,
	// adapter,
	// index,
	// i == 0);
	// i++;
	// }
	// return new IndexCompositeWriter(
	// writers);
	// }
	//
	//
	//
	// @Override
	// public boolean delete(
	// final QueryOptions queryOptions,
	// final Query query ) {
	//
	// try (CloseableIterator<?> it = query(
	// queryOptions,
	// query,
	// true)) {
	// while (it.hasNext()) {
	// it.next();
	// it.remove();
	// }
	// }
	// catch (final IOException e) {
	// LOGGER.error(
	// "Failed deletetion",
	// e);
	// return false;
	// }
	//
	// return true;
	// }
	//
	// /**
	// * Returns all data in this data store that matches the query parameter
	// * within the index described by the index passed in and matches the
	// adapter
	// * (the same adapter ID as the ID ingested). All data that matches the
	// * query, adapter ID, and is in the index ID will be returned as an
	// instance
	// * of the native data type that this adapter supports. The iterator will
	// * only return as many results as the limit passed in.
	// *
	// * @param queryOptions
	// * additional options for the processing the query
	// * @param the
	// * data constraints for the query
	// * @return An iterator on all results that match the query. The iterator
	// * implements Closeable and it is best practice to close the
	// * iterator after it is no longer needed.
	// */
//	@Override
//	public <T> CloseableIterator<T> query(
//			final QueryOptions queryOptions,
//			final Query query ) {
//		return query(
//				queryOptions,
//				query,
//				false);
//	}
//
//	private CloseableIterator query(
//			final QueryOptions queryOptions,
//			final Query query,
//			final boolean isDelete ) {
//		final DedupeFilter filter = new DedupeFilter();
//		filter.setDedupAcrossIndices(
//				false);
//		try {
//			// keep a list of adapters that have been queried, to only low an
//			// adapter to be queried
//			// once
//			final Set<ByteArrayId> queriedAdapters = new HashSet<ByteArrayId>();
//
//			final List<CloseableIterator<Object>> results = new ArrayList<CloseableIterator<Object>>();
//
//			for (final Pair<PrimaryIndex, List<DataAdapter<Object>>> indexAdapterPair : queryOptions
//					.getIndicesForAdapters(
//							adapterStore,
//							adapterIndexMappingStore,
//							indexStore)) {
//				for (final DataAdapter<Object> adapter : indexAdapterPair.getRight()) {
//
//					final boolean firstTimeForAdapter = queriedAdapters.add(
//							adapter.getAdapterId());
//					if (!(firstTimeForAdapter || isDelete)) {
//						continue;
//					}
//
//					final DataStoreCallbackManager callbackManager = new DataStoreCallbackManager(
//							statsStore,
//							secondaryIndexDataStore,
//							firstTimeForAdapter);
//
//					populateResults(
//							results,
//							adapter,
//							indexAdapterPair.getLeft(),
//							query,
//							isDelete ? null : filter,
//							queryOptions,
//							isDelete,
//							callbackManager);
//				}
//			}
//			return new CloseableIteratorWrapper(
//					new Closeable() {
//						@Override
//						public void close()
//								throws IOException {
//							for (final CloseableIterator<?> result : results) {
//								result.close();
//							}
//						}
//					},
//					Iterators.concat(
//							results.iterator()),
//					queryOptions.getLimit());
//
//		}
//		catch (final IOException e)
//
//		{
//			LOGGER.error(
//					"Cannot process query [" + (query == null ? "all" : query.toString()) + "]",
//					e);
//			return new CloseableIterator.Empty();
//		}
//
//	}
//
//	private void populateResults(
//			final List<CloseableIterator<Object>> results,
//			final DataAdapter<Object> adapter,
//			final PrimaryIndex index,
//			final Query query,
//			final DedupeFilter filter,
//			final QueryOptions queryOptions,
//			final boolean isDelete,
//			final DataStoreCallbackManager callbackCache ) {
//		final TreeSet<MemoryStoreEntry> set = ((MemoryStoreEntry) baseOperations).getRowsForIndex(
//				index.getId());
//		final Iterator<MemoryStoreEntry> rowIt = ((TreeSet<MemoryStoreEntry>) set.clone()).iterator();
//		final List<QueryFilter> filters = (query == null) ? new ArrayList<QueryFilter>() : new ArrayList<QueryFilter>(
//				query.createFilters(
//						index.getIndexModel()));
//		filters.add(
//				new QueryFilter() {
//					@Override
//					public boolean accept(
//							final CommonIndexModel indexModel,
//							final IndexedPersistenceEncoding persistenceEncoding ) {
//						if (adapter.getAdapterId().equals(
//								persistenceEncoding.getAdapterId())) {
//							return true;
//						}
//						return false;
//					}
//				});
//		if (filter != null) {
//			filters.add(
//					filter);
//		}
//
//		results.add(
//				new CloseableIterator() {
//					MemoryStoreEntry nextRow = null;
//					MemoryStoreEntry currentRow = null;
//					IndexedAdapterPersistenceEncoding encoding = null;
//
//					private boolean getNext() {
//						while ((nextRow == null) && rowIt.hasNext()) {
//							final MemoryStoreEntry row = rowIt.next();
//							final DataAdapter<?> adapter = adapterStore.getAdapter(
//									new ByteArrayId(
//											row.getTableRowId().getAdapterId()));
//							encoding = MemoryStoreUtils.getEncoding(
//									index.getIndexModel(),
//									adapter,
//									row);
//							boolean ok = true;
//							for (final QueryFilter filter : filters) {
//								if (!filter.accept(
//										index.getIndexModel(),
//										encoding)) {
//									ok = false;
//									break;
//								}
//							}
//							ok &= isAuthorized(
//									row,
//									queryOptions.getAuthorizations());
//							if (ok) {
//								nextRow = row;
//								break;
//							}
//						}
//						return (nextRow != null);
//					}
//
//					@Override
//					public boolean hasNext() {
//						return getNext();
//					}
//
//					@Override
//					public Object next() {
//						currentRow = nextRow;
//						if (isDelete) {
//							final DataAdapter adapter = adapterStore.getAdapter(
//									encoding.getAdapterId());
//							if (adapter instanceof WritableDataAdapter) {
//								callbackCache.getDeleteCallback(
//										(WritableDataAdapter) adapter,
//										index).entryDeleted(
//												currentRow.getInfo(),
//												currentRow.entry);
//							}
//						}
//						((ScanCallback) queryOptions.getScanCallback()).entryScanned(
//								currentRow.getInfo(),
//								currentRow,
//								currentRow.entry);
//						nextRow = null;
//						return currentRow.entry;
//					}
//
//					@Override
//					public void remove() {
//						if (currentRow != null) {
//							set.remove(
//									currentRow);
//						}
//					}
//
//					@Override
//					public void close()
//							throws IOException {
//						final ScanCallback<?, ?> callback = queryOptions.getScanCallback();
//						if ((callback != null) && (callback instanceof Closeable)) {
//							((Closeable) callback).close();
//						}
//					}
//				});
//		final boolean isAggregation = (queryOptions.getAggregation() != null);
//		if (isAggregation) {
//			final Aggregation agg = queryOptions.getAggregation().getRight();
//			for (final CloseableIterator r : results) {
//				while (r.hasNext()) {
//					final Object entry = r.next();
//					if (agg instanceof CommonIndexAggregation) {
//						agg.aggregate(
//								adapter.encode(
//										entry,
//										index.getIndexModel()));
//					}
//					else {
//						agg.aggregate(
//								entry);
//					}
//				}
//				try {
//					r.close();
//				}
//				catch (final IOException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
//			}
//
//			results.clear();
//			results.add(
//					new CloseableIterator.Wrapper<>(
//							Iterators.singletonIterator(
//									(Object) agg.getResult())));
//		}
//	}
//
//
//	private boolean isAuthorized(
//			final MemoryStoreEntry row,
//			final String... authorizations ) {
//		for (final GeoWaveValue value : row.getRow().getFieldValues()) {
//			if (!MemoryStoreUtils.isAuthorized(
//					value.getVisibility(),
//					authorizations)) {
//				return false;
//			}
//		}
//		return true;
//	}

//	public AdapterStore getAdapterStore() {
//		return adapterStore;
//	}
//
//	public IndexStore getIndexStore() {
//		return indexStore;
//	}
}