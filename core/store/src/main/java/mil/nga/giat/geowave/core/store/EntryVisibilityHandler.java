package mil.nga.giat.geowave.core.store;

import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;

public interface EntryVisibilityHandler<T>
{
	public byte[] getVisibility(
			T entry,
			GeoWaveKeyValue... kvs );
}
