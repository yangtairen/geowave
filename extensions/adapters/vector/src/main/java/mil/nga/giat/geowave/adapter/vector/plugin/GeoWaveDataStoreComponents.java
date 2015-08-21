package mil.nga.giat.geowave.adapter.vector.plugin;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.plugin.transaction.GeoWaveTransaction;
import mil.nga.giat.geowave.adapter.vector.query.TransformingVisibilityQuery;
import mil.nga.giat.geowave.adapter.vector.transaction.TransactionsAllocater;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.IngestCallback;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.index.Index;

import org.opengis.feature.simple.SimpleFeature;

public class GeoWaveDataStoreComponents
{
	private final FeatureDataAdapter adapter;
	private final DataStore dataStore;
	private final DataStatisticsStore dataStatisticsStore;
	private final GeoWaveGTDataStore GTstore;
	private final TransactionsAllocater transactionsAllocater;

	private final Index currentIndex;

	public GeoWaveDataStoreComponents(
			final DataStore dataStore,
			final DataStatisticsStore dataStatisticsStore,
			final GeoWaveGTDataStore GTstore,
			final FeatureDataAdapter adapter,
			final TransactionsAllocater transactionsAllocater ) {
		this.adapter = adapter;
		this.dataStore = dataStore;
		this.dataStatisticsStore = dataStatisticsStore;
		this.GTstore = GTstore;
		currentIndex = GTstore.getIndex(adapter);
		this.transactionsAllocater = transactionsAllocater;
	}

	public FeatureDataAdapter getAdapter() {
		return adapter;
	}

	public DataStore getDataStore() {
		return dataStore;
	}

	public GeoWaveGTDataStore getGTstore() {
		return GTstore;
	}

	public Index getCurrentIndex() {
		return currentIndex;
	}

	@SuppressWarnings("unchecked")
	public Map<ByteArrayId, DataStatistics<SimpleFeature>> getDataStatistics(
			final GeoWaveTransaction transaction ) {
		final Map<ByteArrayId, DataStatistics<SimpleFeature>> stats = new HashMap<ByteArrayId, DataStatistics<SimpleFeature>>();

		for (final ByteArrayId statsId : adapter.getSupportedStatisticsIds()) {
			@SuppressWarnings("unused")
			final DataStatistics<SimpleFeature> put = stats.put(
					statsId,
					(DataStatistics<SimpleFeature>) dataStatisticsStore.getDataStatistics(
							adapter.getAdapterId(),
							statsId,
							transaction.composeAuthorizations()));
		}
		return stats;
	}

	public void remove(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {

		dataStore.deleteEntry(
				currentIndex,
				adapter.getDataId(feature),
				adapter.getAdapterId(),
				transaction.composeAuthorizations());
	}

	public void remove(
			final String fid,
			final GeoWaveTransaction transaction )
			throws IOException {

		dataStore.deleteEntry(
				currentIndex,
				new ByteArrayId(
						StringUtils.stringToBinary(fid)),
				adapter.getAdapterId(),
				transaction.composeAuthorizations());
	}

	@SuppressWarnings("unchecked")
	public List<ByteArrayId> write(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {
		return dataStore.ingest(
				adapter,
				currentIndex,
				feature,
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())));
	}

	@SuppressWarnings("unchecked")
	public void write(
			final Iterator<SimpleFeature> featureIt,
			final Map<String, List<ByteArrayId>> fidList,
			final GeoWaveTransaction transaction )
			throws IOException {
		dataStore.ingest(
				adapter,
				currentIndex,
				featureIt,
				new IngestCallback<SimpleFeature>() {

					@Override
					public void entryIngested(
							final DataStoreEntryInfo entryInfo,
							final SimpleFeature entry ) {
						fidList.put(
								entry.getID(),
								entryInfo.getRowIds());
					}

				},
				new UniformVisibilityWriter<SimpleFeature>(
						new GlobalVisibilityHandler(
								transaction.composeVisibility())));
	}

	public List<ByteArrayId> writeCommit(
			final SimpleFeature feature,
			final GeoWaveTransaction transaction )
			throws IOException {
		return dataStore.ingest(
				adapter,
				currentIndex,
				feature);
	}

	public String getTransaction()
			throws IOException {
		return transactionsAllocater.getTransaction();
	}

	public void releaseTransaction(
			final String txID )
			throws IOException {
		transactionsAllocater.releaseTransaction(txID);
	}
}
