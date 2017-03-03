package mil.nga.giat.geowave.core.store.base;

import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;

public interface Deleter<R extends GeoWaveKeyValue> extends
		AutoCloseable
{
	public void delete(
			R row,
			DataAdapter<?> adapter );
}
