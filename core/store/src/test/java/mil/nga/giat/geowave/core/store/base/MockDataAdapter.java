package mil.nga.giat.geowave.core.store.base;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class MockDataAdapter implements
		WritableDataAdapter<MockEntry>
{
	public final static ByteArrayId ADAPTER_ID = new ByteArrayId(
			"mock");
	private final static ByteArrayId MOCK_FIELD_ID = new ByteArrayId(
			"mock");
	private final FieldVisibilityHandler<MockEntry, Object> distortionVisibilityHandler;

	public MockDataAdapter() {
		this(
				null);
	}

	public MockDataAdapter(
			final FieldVisibilityHandler<MockEntry, Object> distortionVisibilityHandler ) {
		this.distortionVisibilityHandler = distortionVisibilityHandler;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return ADAPTER_ID;
	}

	@Override
	public boolean isSupported(
			final MockEntry entry ) {
		return true;
	}

	@Override
	public ByteArrayId getDataId(
			final MockEntry entry ) {
		return entry.getDataId();
	}

	@Override
	public MockEntry decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		return new MockEntry(
				data.getDataId().getString(),
				(Double) data.getAdapterExtendedData().getValue(
						MOCK_FIELD_ID));
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final MockEntry entry,
			final CommonIndexModel indexModel ) {
		final Map<ByteArrayId, Object> fieldIdToValueMap = new HashMap<ByteArrayId, Object>();
		fieldIdToValueMap.put(
				MOCK_FIELD_ID,
				entry.getMockValue());
		return new AdapterPersistenceEncoding(
				getAdapterId(),
				entry.getDataId(),
				new PersistentDataset<CommonIndexValue>(),
				new PersistentDataset<Object>(
						fieldIdToValueMap));
	}

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		if (MOCK_FIELD_ID.equals(fieldId)) {
			return (FieldReader) FieldUtils.getDefaultReaderForClass(Double.class);
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public FieldWriter<MockEntry, Object> getWriter(
			final ByteArrayId fieldId ) {
		if (MOCK_FIELD_ID.equals(fieldId)) {
			if (distortionVisibilityHandler != null) {
				return (FieldWriter) FieldUtils.getDefaultWriterForClass(
						Double.class,
						distortionVisibilityHandler);
			}
			else {
				return (FieldWriter) FieldUtils.getDefaultWriterForClass(Double.class);
			}
		}
		return null;
	}

	@Override
	public int getPositionOfOrderedField(
			final CommonIndexModel model,
			final ByteArrayId fieldId ) {
		int i = 0;
		for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
			if (fieldId.equals(dimensionField.getFieldId())) {
				return i;
			}
			i++;
		}
		if (fieldId.equals(MOCK_FIELD_ID)) {
			return i;
		}
		return -1;
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			final CommonIndexModel model,
			final int position ) {
		if (position < model.getDimensions().length) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (i == position) {
					return dimensionField.getFieldId();
				}
				i++;
			}
		}
		else {
			final int numDimensions = model.getDimensions().length;
			if (position == numDimensions) {
				return MOCK_FIELD_ID;
			}
		}
		return null;
	}

}
