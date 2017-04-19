package mil.nga.giat.geowave.core.store.base;

import java.util.Iterator;

import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;

public interface Reader extends
		AutoCloseable,
		Iterator<GeoWaveRow>
{

}
