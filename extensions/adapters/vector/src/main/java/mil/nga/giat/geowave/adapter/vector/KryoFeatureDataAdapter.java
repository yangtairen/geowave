package mil.nga.giat.geowave.adapter.vector;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

public class KryoFeatureDataAdapter implements
		DataAdapter<SimpleFeature>
{
	private final SimpleFeatureType featureType;
	private final ByteArrayId adapterId;

	public KryoFeatureDataAdapter(
			final SimpleFeatureType featureType ) {
		super();
		this.featureType = featureType;
		adapterId = new ByteArrayId(
				StringUtils.stringToBinary(
						featureType.getTypeName()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		final Class<?> clazz = SimpleFeature.class;
		return (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(
				clazz);
	}

	@Override
	public ByteArrayId getAdapterId() {
		return adapterId;
	}

	@Override
	public boolean isSupported(
			final SimpleFeature entry ) {
		return entry.getName().getURI().equals(
				featureType.getName().getURI());
	}

	@Override
	public ByteArrayId getDataId(
			final SimpleFeature entry ) {
		return new ByteArrayId(
				StringUtils.stringToBinary(
						entry.getID()));
	}

	@Override
	public SimpleFeature decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] toBinary() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		// TODO Auto-generated method stub

	}

}
