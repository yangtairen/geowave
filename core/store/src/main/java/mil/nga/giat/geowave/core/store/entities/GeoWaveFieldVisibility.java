package mil.nga.giat.geowave.core.store.entities;

public interface GeoWaveFieldVisibility
{
	public byte[] getFieldMask();

	public byte[] getVisibility();
}
