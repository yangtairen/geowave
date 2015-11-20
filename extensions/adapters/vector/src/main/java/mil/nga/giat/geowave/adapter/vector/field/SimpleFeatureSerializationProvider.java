package mil.nga.giat.geowave.adapter.vector.field;

import org.opengis.feature.simple.SimpleFeature;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldSerializationProviderSpi;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;

public class SimpleFeatureSerializationProvider implements
		FieldSerializationProviderSpi<SimpleFeature>
{

	@Override
	public FieldReader<SimpleFeature> getFieldReader() {
		return new SimpleFeatureReader();
	}

	@Override
	public FieldWriter<Object, SimpleFeature> getFieldWriter() {
		return new SimpleFeatureWriter();
	}

	protected static class SimpleFeatureReader implements
			FieldReader<SimpleFeature>
	{

		@Override
		public SimpleFeature readField(
				final byte[] fieldData ) {
			if (fieldData == null) {
				return null;
			}
			return null; // FIXME use Kryo to deserialize SimpleFeature
		}

	}

	protected static class SimpleFeatureWriter implements
			FieldWriter<Object, SimpleFeature>
	{

		@Override
		public byte[] writeField(
				final SimpleFeature fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return null; // FIXME use Kryo to serialize SimpleFeature
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final SimpleFeature fieldValue ) {
			return new byte[] {};
		}

	}

}
