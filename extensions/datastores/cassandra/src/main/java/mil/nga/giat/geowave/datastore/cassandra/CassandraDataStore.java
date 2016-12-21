package mil.nga.giat.geowave.datastore.cassandra;

import java.io.Closeable;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;

public class CassandraDataStore extends
		BaseDataStore
{

	public CassandraDataStore(
			IndexStore indexStore,
			AdapterStore adapterStore,
			DataStatisticsStore statisticsStore,
			AdapterIndexMappingStore indexMappingStore,
			SecondaryIndexDataStore secondaryIndexDataStore,
			DataStoreOperations operations,
			DataStoreOptions options ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				operations,
				options);
	}

	@Override
	protected boolean deleteAll(
			String tableName,
			String columnFamily,
			String... additionalAuthorizations ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected void addToBatch(
			Closeable idxDeleter,
			List<ByteArrayId> rowIds )
			throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Closeable createIndexDeleter(
			String indexTableName,
			String[] authorizations )
			throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected List<ByteArrayId> getAltIndexRowIds(
			String altIdxTableName,
			List<ByteArrayId> dataIds,
			ByteArrayId adapterId,
			String... authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIterator<Object> getEntryRows(
			PrimaryIndex index,
			AdapterStore tempAdapterStore,
			List<ByteArrayId> dataIds,
			DataAdapter<?> adapter,
			ScanCallback<Object> callback,
			DedupeFilter dedupeFilter,
			String[] authorizations ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIterator<Object> queryConstraints(
			List<ByteArrayId> adapterIdsToQuery,
			PrimaryIndex index,
			Query sanitizedQuery,
			DedupeFilter filter,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIterator<Object> queryRowPrefix(
			PrimaryIndex index,
			ByteArrayId rowPrefix,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore,
			List<ByteArrayId> adapterIdsToQuery ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected CloseableIterator<Object> queryRowIds(
			DataAdapter<Object> adapter,
			PrimaryIndex index,
			List<ByteArrayId> rowIds,
			DedupeFilter filter,
			QueryOptions sanitizedQueryOptions,
			AdapterStore tempAdapterStore ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected <T> void addAltIndexCallback(
			List<IngestCallback<T>> callbacks,
			String indexName,
			DataAdapter<T> adapter,
			ByteArrayId primaryIndexId ) {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected IndexWriter createIndexWriter(
			DataAdapter adapter,
			PrimaryIndex index,
			DataStoreOperations baseOperations,
			DataStoreOptions baseOptions,
			IngestCallback callback,
			Closeable closable ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void initOnIndexWriterCreate(
			DataAdapter adapter,
			PrimaryIndex index ) {
		// TODO Auto-generated method stub
		
	}

}
