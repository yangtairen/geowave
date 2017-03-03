package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.callback.DeleteCallback;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;

public class DataStatisticsBuilder<T, R extends GeoWaveKeyValue> implements
		IngestCallback<T,R>,
		DeleteCallback<T,R>,
		ScanCallback<T, R>
{
	private final StatisticsProvider<T> statisticsProvider;
	private final Map<ByteArrayId, DataStatistics<T>> statisticsMap = new HashMap<ByteArrayId, DataStatistics<T>>();
	private final ByteArrayId statisticsId;
	private final EntryVisibilityHandler<T> visibilityHandler;
	private static final Logger LOGGER = Logger.getLogger(DataStatistics.class);

	public DataStatisticsBuilder(
			final StatisticsProvider<T> statisticsProvider,
			final ByteArrayId statisticsId ) {
		this.statisticsProvider = statisticsProvider;
		this.statisticsId = statisticsId;
		this.visibilityHandler = statisticsProvider.getVisibilityHandler(statisticsId);
	}

	@Override
	public void entryIngested(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		final ByteArrayId visibility = new ByteArrayId(
				visibilityHandler.getVisibility(
						entryInfo,
						entry));
		DataStatistics<T> statistics = statisticsMap.get(visibility);
		if (statistics == null) {
			statistics = statisticsProvider.createDataStatistics(statisticsId);
			if (statistics == null) {
				return;
			}
			statistics.setVisibility(visibility.getBytes());
			statisticsMap.put(
					visibility,
					statistics);
		}
		statistics.entryIngested(
				entryInfo,
				entry);
	}

	public Collection<DataStatistics<T>> getStatistics() {
		return statisticsMap.values();
	}

	@SuppressWarnings("unchecked")
	@Override
	public void entryDeleted(
			final DataStoreEntryInfo entryInfo,
			final T entry ) {
		final ByteArrayId visibilityByteArray = new ByteArrayId(
				visibilityHandler.getVisibility(
						entryInfo,
						entry));
		DataStatistics<T> statistics = statisticsMap.get(visibilityByteArray);
		if (statistics == null) {
			statistics = statisticsProvider.createDataStatistics(statisticsId);
			statistics.setVisibility(visibilityByteArray.getBytes());
			statisticsMap.put(
					visibilityByteArray,
					statistics);
		}
		if (statistics instanceof DeleteCallback) {
			((DeleteCallback<T>) statistics).entryDeleted(
					entryInfo,
					entry);
		}
	}

	@Override
	public void entryScanned(
			DataStoreEntryInfo entryInfo,
			Object nativeDataStoreObj,
			T entry ) {
		final ByteArrayId visibility = new ByteArrayId(
				visibilityHandler.getVisibility(
						entryInfo,
						entry));
		DataStatistics<T> statistics = statisticsMap.get(visibility);
		if (statistics == null) {
			statistics = statisticsProvider.createDataStatistics(statisticsId);
			if (statistics == null) {
				return;
			}
			statistics.setVisibility(visibility.getBytes());
			statisticsMap.put(
					visibility,
					statistics);
		}
		statistics.entryIngested(
				entryInfo,
				entry);

	}
}
