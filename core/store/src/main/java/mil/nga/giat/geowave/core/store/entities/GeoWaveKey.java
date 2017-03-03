package mil.nga.giat.geowave.core.store.entities;

public interface GeoWaveKey
{
	public byte[] getDataId();

	public byte[] getAdapterId();

	public byte[] getFieldMask();

	public byte[] getSortKey();
	
	public byte[] getPartitionKey();

	public byte[] getVisibility();

	public int getNumberOfDuplicates();
}
