package mil.nga.giat.geowave.core.store.base;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStoreStatisticsProvider;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatisticsProvider;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.DeleteCallbackList;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallbackList;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataAdapter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataManager;
import mil.nga.giat.geowave.core.store.index.SecondaryIndexDataStore;

public class DataStoreCallbackManager
{

	final private DataStatisticsStore statsStore;
	private boolean persistStats = true;
	final private SecondaryIndexDataStore secondaryIndexStore;

	final private boolean captureAdapterStats;

	final Map<ByteArrayId, IngestCallback<?, GeoWaveKeyValue>> icache = new HashMap<ByteArrayId, IngestCallback<?, GeoWaveKeyValue>>();
	final Map<ByteArrayId, DeleteCallback<?, GeoWaveKeyValue>> dcache = new HashMap<ByteArrayId, DeleteCallback<?, GeoWaveKeyValue>>();

	public DataStoreCallbackManager(
			final DataStatisticsStore statsStore,
			final SecondaryIndexDataStore secondaryIndexStore,
			boolean captureAdapterStats ) {
		this.statsStore = statsStore;
		this.secondaryIndexStore = secondaryIndexStore;
		this.captureAdapterStats = captureAdapterStats;
	}

	public <T> IngestCallback<T, GeoWaveKeyValue> getIngestCallback(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index ) {
		if (!icache.containsKey(writableAdapter.getAdapterId())) {
			final DataStoreStatisticsProvider<T> statsProvider = new DataStoreStatisticsProvider<T>(
					writableAdapter,
					index,
					captureAdapterStats);
			final List<IngestCallback<T, GeoWaveKeyValue>> callbackList = new ArrayList<IngestCallback<T, GeoWaveKeyValue>>();
			if ((writableAdapter instanceof StatisticsProvider) && persistStats) {
				callbackList.add(new StatsCompositionTool<T>(
						statsProvider,
						statsStore));
			}
			if (captureAdapterStats && writableAdapter instanceof SecondaryIndexDataAdapter<?>) {
				callbackList.add(new SecondaryIndexDataManager<T>(
						secondaryIndexStore,
						(SecondaryIndexDataAdapter<T>) writableAdapter,
						index.getId()));
			}
			icache.put(
					writableAdapter.getAdapterId(),
					new IngestCallbackList<T, GeoWaveKeyValue>(
							callbackList));
		}
		return (IngestCallback<T, GeoWaveKeyValue>) icache.get(writableAdapter.getAdapterId());

	}

	public void setPersistStats(
			final boolean persistStats ) {
		this.persistStats = persistStats;
	}

	public <T> DeleteCallback<T, GeoWaveKeyValue> getDeleteCallback(
			final WritableDataAdapter<T> writableAdapter,
			final PrimaryIndex index ) {
		if (!dcache.containsKey(writableAdapter.getAdapterId())) {
			final DataStoreStatisticsProvider<T> statsProvider = new DataStoreStatisticsProvider<T>(
					writableAdapter,
					index,
					captureAdapterStats);
			final List<DeleteCallback<T, GeoWaveKeyValue>> callbackList = new ArrayList<DeleteCallback<T, GeoWaveKeyValue>>();
			if ((writableAdapter instanceof StatisticsProvider) && persistStats) {
				callbackList.add(new StatsCompositionTool<T>(
						statsProvider,
						statsStore));
			}
			if (captureAdapterStats && writableAdapter instanceof SecondaryIndexDataAdapter<?>) {
				callbackList.add(new SecondaryIndexDataManager<T>(
						secondaryIndexStore,
						(SecondaryIndexDataAdapter<T>) writableAdapter,
						index.getId()));
			}
			dcache.put(
					writableAdapter.getAdapterId(),
					new DeleteCallbackList<T, GeoWaveKeyValue>(
							callbackList));
		}
		return (DeleteCallback<T, GeoWaveKeyValue>) dcache.get(writableAdapter.getAdapterId());

	}

	public void close()
			throws IOException {
		for (final IngestCallback<?, GeoWaveKeyValue> callback : icache.values()) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
		for (final DeleteCallback<?, GeoWaveKeyValue> callback : dcache.values()) {
			if (callback instanceof Closeable) {
				((Closeable) callback).close();
			}
		}
	}
}
