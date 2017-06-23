package mil.nga.giat.geowave.core.store.metadata;

public interface MetadataDeleter extends
		AutoCloseable
{
	public boolean delete(
			MetadataQuery query );

	public void flush();
}
