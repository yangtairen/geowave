package mil.nga.giat.geowave.adapter.vector;

import java.util.ArrayList;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

// FIXME currently does not re-project (i.e. assumes EPSG:4326)
// FIXME currently does not support stats
// FIXME currently does not support secondary indexing
// FIXME currently does nto support MapReduce
public class KryoFeatureDataAdapter extends
		AbstractDataAdapter<SimpleFeature>
{
	private final SimpleFeatureType featureType;
	private final ByteArrayId adapterId;

	public KryoFeatureDataAdapter(
			final SimpleFeatureType featureType ) {
		super(
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(),
				featureType);
		this.featureType = featureType;
		adapterId = new ByteArrayId(
				StringUtils.stringToBinary(featureType.getTypeName()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		final Class<?> clazz = SimpleFeature.class;
		return (FieldReader<Object>) FieldUtils.getDefaultReaderForClass(clazz);
	}

	@SuppressWarnings("unchecked")
	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {
		final Class<?> clazz = SimpleFeature.class;
		return (FieldWriter<SimpleFeature, Object>) FieldUtils.getDefaultWriterForClass(clazz);
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
				StringUtils.stringToBinary(entry.getID()));
	}

	@Override
	public SimpleFeature decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		return super.decode(
				data,
				index);
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final SimpleFeature entry,
			final CommonIndexModel indexModel ) {
		return super.encode(
				entry,
				indexModel);
	}

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		return new FeatureRowBuilder(
				featureType);
	}

}
