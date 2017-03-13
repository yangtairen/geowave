package mil.nga.giat.geowave.core.store.adapter.statistics;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.EntryVisibilityHandler;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyValue;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class FieldIdStatisticVisibility<T> implements
		EntryVisibilityHandler<T>
{
	private final ByteArrayId fieldId;

	public FieldIdStatisticVisibility(
			final ByteArrayId fieldId,
			final CommonIndexModel model,
			final DataAdapter adapter ) {
		this.fieldId = fieldId;
	}

	@Override
	public byte[] getVisibility(
			final T entry,
			final GeoWaveKeyValue... kvs ) {
		for (final FieldInfo<?> f : entryInfo.getFieldInfo()) {
			if (f.getDataValue().getId().equals(
					fieldId)) {
				return f.getVisibility();
			}
		}
		return null;
	}
}
