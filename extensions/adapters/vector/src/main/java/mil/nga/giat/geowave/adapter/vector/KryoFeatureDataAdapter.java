package mil.nga.giat.geowave.adapter.vector;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

import org.opengis.feature.simple.SimpleFeature;

// TODO currently just a stub
public class KryoFeatureDataAdapter implements
		DataAdapter<SimpleFeature>
{

	@Override
	public FieldReader<Object> getReader(
			ByteArrayId fieldId ) {
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
			byte[] bytes ) {
		// TODO Auto-generated method stub

	}

	@Override
	public ByteArrayId getAdapterId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isSupported(
			SimpleFeature entry ) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public ByteArrayId getDataId(
			SimpleFeature entry ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SimpleFeature decode(
			IndexedAdapterPersistenceEncoding data,
			PrimaryIndex index ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			SimpleFeature entry,
			CommonIndexModel indexModel ) {
		// TODO Auto-generated method stub
		return null;
	}

}
