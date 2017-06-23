package mil.nga.giat.geowave.core.store.metadata;

import mil.nga.giat.geowave.core.store.CloseableIterator;

public interface MetadataReader
{
	public CloseableIterator<GeoWaveMetadata> query(
			MetadataQuery query );
}
