package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;

/**
 * 
 * Supplies not additional visibility
 * 
 * @param <T>
 */
public class EmptyStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{

	@Override
	public byte[] getVisibility(
			final T entry,
			final GeoWaveKeyValue... kvs) {
		return new byte[0];
	}

}
