package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.memory.MemoryAdapterStore;
import mil.nga.giat.geowave.core.store.memory.MemoryDataStatisticsStore;
import mil.nga.giat.geowave.core.store.memory.MemoryIndexStore;
import mil.nga.giat.geowave.core.store.memory.MemorySecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class MockBaseDataStore extends
		BaseDataStore
{

	public MockBaseDataStore(
			final String tableNamespace ) {
		super(
				new MemoryIndexStore(),
				new MemoryAdapterStore(),
				new MemoryDataStatisticsStore(),
				new MemoryAdapterIndexMappingStore(),
				new MemorySecondaryIndexDataStore(),
				new MockDataStoreOperations(
						tableNamespace),
				new BaseDataStoreOptions());
	}

	@Override
	protected boolean deleteAll(
			final String tableName,
			final String columnFamily,
			final String... additionalAuthorizations ) {
		// TODO GEOWAVE-1003, complete this method as necessary for tests
		return false;
	}

	@Override
	protected void addToBatch(
			final Closeable idxDeleter,
			final List<ByteArrayId> rowIds )
			throws Exception {
		// TODO GEOWAVE-1003, complete this method as necessary for tests

	}

	@Override
	protected Closeable createIndexDeleter(
			final String indexTableName,
			final String[] authorizations )
			throws Exception {
		// TODO GEOWAVE-1003, complete this method as necessary for tests
		return null;
	}

	@Override
	protected List<ByteArrayId> getAltIndexRowIds(
			final String altIdxTableName,
			final List<ByteArrayId> dataIds,
			final ByteArrayId adapterId,
			final String... authorizations ) {
		// TODO GEOWAVE-1002, complete this method as necessary for tests
		return null;
	}

	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final ScanCallback<Object> callback,
			final DedupeFilter dedupeFilter,
			final String[] authorizations ) {
		// TODO GEOWAVE-1002, complete this method as necessary for tests
		return null;
	}

	@Override
	protected CloseableIterator<Object> queryConstraints(
			final List<ByteArrayId> adapterIdsToQuery,
			final PrimaryIndex index,
			final Query sanitizedQuery,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		// TODO GEOWAVE-1003, complete this method as necessary for tests
		return null;
	}

	@Override
	protected CloseableIterator<Object> queryRowPrefix(
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> adapterIdsToQuery ) {
		// TODO GEOWAVE-1002, complete this method as necessary for tests
		return null;
	}

	@Override
	protected CloseableIterator<Object> queryRowIds(
			final DataAdapter<Object> adapter,
			final PrimaryIndex index,
			final List<ByteArrayId> rowIds,
			final DedupeFilter filter,
			final QueryOptions sanitizedQueryOptions,
			final AdapterStore tempAdapterStore ) {
		// TODO GEOWAVE-1002, complete this method as necessary for tests
		return null;
	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		// TODO GEOWAVE-1004, complete this method as necessary for tests

	}

	@Override
	protected IndexWriter createIndexWriter(
			final DataAdapter adapter,
			final PrimaryIndex index,
			final DataStoreOperations baseOperations,
			final DataStoreOptions baseOptions,
			final IngestCallback callback,
			final Closeable closable ) {
		// TODO GEOWAVE-1004, complete this method as necessary for tests
		return null;
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {
		// TODO GEOWAVE-1004, complete this method as necessary for tests

	}

}
