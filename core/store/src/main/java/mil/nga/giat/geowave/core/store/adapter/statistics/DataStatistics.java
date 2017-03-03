package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Mergeable;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;

public interface DataStatistics<T> extends
		Mergeable,
		IngestCallback<T, GeoWaveKeyValue>
{
	public ByteArrayId getDataAdapterId();

	public void setDataAdapterId(
			ByteArrayId dataAdapterId );

	public ByteArrayId getStatisticsId();

	public void setVisibility(
			byte[] visibility );

	public byte[] getVisibility();
}
