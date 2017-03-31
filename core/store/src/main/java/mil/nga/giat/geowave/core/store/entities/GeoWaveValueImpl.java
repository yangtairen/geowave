package mil.nga.giat.geowave.core.store.entities;

public class GeoWaveValueImpl implements
		GeoWaveValue
{
	private final byte[] fieldMask;
	private final byte[] visibility;
	private final byte[] value;

	public GeoWaveValueImpl(
			final byte[] fieldMask,
			final byte[] visibility,
			final byte[] value ) {
		this.fieldMask = fieldMask;
		this.visibility = visibility;
		this.value = value;
	}

	@Override
	public byte[] getFieldMask() {
		return fieldMask;
	}

	@Override
	public byte[] getVisibility() {
		return visibility;
	}

	@Override
	public byte[] getValue() {
		return value;
	}
}
