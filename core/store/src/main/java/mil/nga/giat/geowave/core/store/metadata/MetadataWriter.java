package mil.nga.giat.geowave.core.store.metadata;

public interface MetadataWriter extends
		AutoCloseable
{
	public void write(
			GeoWaveMetadata metadata );

	public void flush();
}
