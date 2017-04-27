package mil.nga.giat.geowave.datastore.accumulo;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchDeleter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.log4j.Logger;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.CloseableIteratorWrapper;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.AdapterIndexMappingStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.RowMergingDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DuplicateEntryCount;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.base.IntermediaryReadEntryInfo;
import mil.nga.giat.geowave.core.store.base.Deleter;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.IndexMetaDataSet;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexUtils;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.core.store.util.DataAdapterAndIndexCache;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.AccumuloSplitsProvider;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.GeoWaveAccumuloRecordReader;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloOptions;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowIdsQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.AccumuloRowPrefixQuery;
import mil.nga.giat.geowave.datastore.accumulo.query.SingleEntryFilterIterator;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloEntryIteratorWrapper;
import mil.nga.giat.geowave.datastore.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.datastore.accumulo.util.ScannerClosableWrapper;
import mil.nga.giat.geowave.mapreduce.MapReduceDataStore;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputKey;

/**
 * This is the Accumulo implementation of the data store. It requires an
 * AccumuloOperations instance that describes how to connect (read/write data)
 * to Apache Accumulo. It can create default implementations of the IndexStore
 * and AdapterStore based on the operations which will persist configuration
 * information to Accumulo tables, or an implementation of each of these stores
 * can be passed in A DataStore can both ingest and query data based on
 * persisted indices and data adapters. When the data is ingested it is
 * explicitly given an index and a data adapter which is then persisted to be
 * used in subsequent queries.
 */
public class AccumuloDataStore extends
		BaseDataStore<AccumuloRow> implements
		MapReduceDataStore
{
	public final static String TYPE = "accumulo";

	private final static Logger LOGGER = Logger.getLogger(AccumuloDataStore.class);

	private final AccumuloOperations accumuloOperations;
	private final AccumuloOptions accumuloOptions;

	private final AccumuloSplitsProvider splitsProvider = new AccumuloSplitsProvider();

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations ) {
		this(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations),
				new AccumuloAdapterIndexMappingStore(
						accumuloOperations),
				accumuloOperations);
	}

	public AccumuloDataStore(
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		this(
				new AccumuloIndexStore(
						accumuloOperations),
				new AccumuloAdapterStore(
						accumuloOperations),
				new AccumuloDataStatisticsStore(
						accumuloOperations),
				new AccumuloSecondaryIndexDataStore(
						accumuloOperations,
						accumuloOptions),
				new AccumuloAdapterIndexMappingStore(
						accumuloOperations),
				accumuloOperations,
				accumuloOptions);
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations ) {
		this(
				indexStore,
				adapterStore,
				statisticsStore,
				secondaryIndexDataStore,
				indexMappingStore,
				accumuloOperations,
				new AccumuloOptions());
	}

	public AccumuloDataStore(
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statisticsStore,
			final AccumuloSecondaryIndexDataStore secondaryIndexDataStore,
			final AdapterIndexMappingStore indexMappingStore,
			final AccumuloOperations accumuloOperations,
			final AccumuloOptions accumuloOptions ) {
		super(
				indexStore,
				adapterStore,
				statisticsStore,
				indexMappingStore,
				secondaryIndexDataStore,
				accumuloOperations,
				accumuloOptions);

		this.accumuloOperations = accumuloOperations;
		this.accumuloOptions = accumuloOptions;
		secondaryIndexDataStore.setDataStore(this);
	}

	@Override
	protected void initOnIndexWriterCreate(
			final DataAdapter adapter,
			final PrimaryIndex index ) {

		final String indexName = index.getId().getString();

		try {
			if (adapter instanceof RowMergingDataAdapter) {
				if (!DataAdapterAndIndexCache.getInstance(
						RowMergingAdapterOptionProvider.ROW_MERGING_ADAPTER_CACHE_ID).add(
						adapter.getAdapterId(),
						indexName)) {
					AccumuloUtils.attachRowMergingIterators(
							((RowMergingDataAdapter<?, ?>) adapter),
							accumuloOperations,
							accumuloOptions,
							index.getIndexStrategy().getPartitionKeys(),
							indexName);
				}
			}

			final byte[] adapterId = adapter.getAdapterId().getBytes();
			if (accumuloOptions.isUseLocalityGroups() && !accumuloOperations.localityGroupExists(
					indexName,
					adapterId)) {
				accumuloOperations.addLocalityGroup(
						indexName,
						adapterId);
			}
		}
		catch (AccumuloException | TableNotFoundException | AccumuloSecurityException e) {
			LOGGER.error(
					"Unable to determine existence of locality group [" + adapter.getAdapterId().getString() + "]",
					e);
		}

	}

	@Override
	protected <T> void addAltIndexCallback(
			final List<IngestCallback<T>> callbacks,
			final String indexName,
			final DataAdapter<T> adapter,
			final ByteArrayId primaryIndexId ) {
		try {
			callbacks.add(new AltIndexCallback<T>(
					indexName,
					(WritableDataAdapter<T>) adapter,
					primaryIndexId));

		}
		catch (final Exception e) {
			LOGGER.error(
					"Unable to create table table for alt index to  [" + indexName + "]",
					e);
		}
	}

	private class AltIndexCallback<T> implements
			IngestCallback<T>
	{
		private final ByteArrayId EMPTY_VISIBILITY = new ByteArrayId(
				new byte[0]);
		private final ByteArrayId EMPTY_FIELD_ID = new ByteArrayId(
				new byte[0]);
		private final WritableDataAdapter<T> adapter;
		private final String altIdxTableName;
		private final ByteArrayId primaryIndexId;
		private final ByteArrayId altIndexId;

		public AltIndexCallback(
				final String indexName,
				final WritableDataAdapter<T> adapter,
				final ByteArrayId primaryIndexId )
				throws TableNotFoundException {
			this.adapter = adapter;
			altIdxTableName = indexName + ALT_INDEX_TABLE;
			altIndexId = new ByteArrayId(
					altIdxTableName);
			this.primaryIndexId = primaryIndexId;
			try {
				if (accumuloOperations.indexExists(new ByteArrayId(indexName))) {
					if (!accumuloOperations.indexExists(new ByteArrayId(altIdxTableName))) {
						throw new TableNotFoundException(
								altIdxTableName,
								altIdxTableName,
								"Requested alternate index table does not exist.");
					}
				}
				else {
					// index table does not exist yet
					if (accumuloOperations.indexExists(new ByteArrayId(altIdxTableName))) {
						accumuloOperations.deleteTable(altIdxTableName);
						LOGGER.warn("Deleting current alternate index table [" + altIdxTableName
								+ "] as main table does not yet exist.");
					}
				}
			}
			catch (final IOException e) {
				LOGGER.error("Exception checking for index " + indexName + ": " + e);
			}
		}

		@Override
		public void entryIngested(
				final T entry,
				GeoWaveRow... geowaveRows) {
			for (final GeoWaveRow geowaveRow : geowaveRows) {
				final ByteArrayId dataId = adapter.getDataId(entry);
				if ((dataId != null) && (dataId.getBytes() != null) && (dataId.getBytes().length > 0)) {
					secondaryIndexDataStore.storeJoinEntry(
							altIndexId,
							dataId,
							adapter.getAdapterId(),
							EMPTY_FIELD_ID,
							primaryIndexId,
							new ByteArrayId(geowaveRow.getPartitionKey()),
							new ByteArrayId(geowaveRow.getSortKey()),
							EMPTY_VISIBILITY);
				}
			}
		}
	}

	@SuppressFBWarnings(value = "DLS_DEAD_LOCAL_STORE", justification = "i is part of loop condition")
	@Override
	protected CloseableIterator<Object> getEntryRows(
			final PrimaryIndex index,
			final AdapterStore tempAdapterStore,
			final List<ByteArrayId> dataIds,
			final DataAdapter<?> adapter,
			final DedupeFilter dedupeFilter,
			final QueryOptions queryOptions ) {
		try {
			final ScannerBase scanner = accumuloOperations.createScanner(
					index.getId().getString(),
					queryOptions.getAuthorizations());
			final DifferingFieldVisibilityEntryCount visibilityCount = DifferingFieldVisibilityEntryCount
					.getVisibilityCounts(
							index,
							Collections.singletonList(adapter.getAdapterId()),
							statisticsStore,
							queryOptions.getAuthorizations());
			scanner.fetchColumnFamily(new Text(
					adapter.getAdapterId().getBytes()));
			if (visibilityCount.isAnyEntryDifferingFieldVisiblity()) {
				final IteratorSetting rowIteratorSettings = new IteratorSetting(
						SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_PRIORITY,
						SingleEntryFilterIterator.WHOLE_ROW_ITERATOR_NAME,
						WholeRowIterator.class);
				scanner.addScanIterator(rowIteratorSettings);

			}
			final IteratorSetting filterIteratorSettings = new IteratorSetting(
					SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_PRIORITY,
					SingleEntryFilterIterator.ENTRY_FILTER_ITERATOR_NAME,
					SingleEntryFilterIterator.class);

			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.ADAPTER_ID,
					ByteArrayUtils.byteArrayToString(adapter.getAdapterId().getBytes()));

			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.WHOLE_ROW_ENCODED_KEY,
					Boolean.toString(visibilityCount.isAnyEntryDifferingFieldVisiblity()));
			filterIteratorSettings.addOption(
					SingleEntryFilterIterator.DATA_IDS,
					SingleEntryFilterIterator.encodeIDs(dataIds));
			scanner.addScanIterator(filterIteratorSettings);

			return new CloseableIteratorWrapper<Object>(
					new ScannerClosableWrapper(
							scanner),
					new AccumuloEntryIteratorWrapper(
							visibilityCount.isAnyEntryDifferingFieldVisiblity(),
							this,
							adapterStore,
							index,
							scanner.iterator(),
							dedupeFilter,
							null));

		}
		catch (final TableNotFoundException e) {
			LOGGER.warn(
					"Unable to query table '" + index.getId().getString() + "'.  Table does not exist.",
					e);
		}

		return null;
	}

	@Override
	public List<InputSplit> getSplits(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final Integer minSplits,
			final Integer maxSplits )
			throws IOException,
			InterruptedException {
		return splitsProvider.getSplits(
				accumuloOperations,
				query,
				queryOptions,
				adapterStore,
				statsStore,
				indexStore,
				indexMappingStore,
				minSplits,
				maxSplits);
	}

	@Override
	public RecordReader<GeoWaveInputKey, ?> createRecordReader(
			final DistributableQuery query,
			final QueryOptions queryOptions,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final IndexStore indexStore,
			final boolean isOutputWritable,
			final InputSplit inputSplit )
			throws IOException,
			InterruptedException {
		return new GeoWaveAccumuloRecordReader(
				query,
				queryOptions,
				isOutputWritable,
				adapterStore,
				this,
				accumuloOperations);
	}
}
