package mil.nga.giat.geowave.adapter.vector.query.cql;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class CQLQueryFilter implements
		DistributableQueryFilter
{
	private String cql;
	private FeatureDataAdapter adapter;

	protected CQLQueryFilter() {
		super();
	}

	public CQLQueryFilter(
			final String cql,
			final FeatureDataAdapter adapter ) {
		this.cql = cql;
		this.adapter = adapter;
	}

	@Override
	public boolean accept(
			final IndexedPersistenceEncoding persistenceEncoding ) {
		if (adapter.getAdapterId().equals(
				persistenceEncoding.getAdapterId())) {
			final PersistentDataset<byte[]> stillUnknownValues = new PersistentDataset<byte[]>();
			final PersistentDataset<Object> adapterExtendedValues = new PersistentDataset<Object>();
			for (final PersistentValue<byte[]> v : persistenceEncoding.getUnknownData().getValues()) {
				final FieldReader<Object> reader = adapter.getReader(v.getId());
				final Object value = reader.readField(v.getValue());
				adapterExtendedValues.addValue(new PersistentValue<Object>(
						v.getId(),
						value));
			}
			final IndexedAdapterPersistenceEncoding encoding = new IndexedAdapterPersistenceEncoding(
					persistenceEncoding.getAdapterId(),
					persistenceEncoding.getDataId(),
					persistenceEncoding.getIndexInsertionId(),
					persistenceEncoding.getDuplicateCount(),
					persistenceEncoding.getCommonData(),
					stillUnknownValues,
					adapterExtendedValues);
			// TODO: wrap this up, need commonindexmodel
			// adapter.decode(
			// persistenceEncoding,
			// new Index());
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}
}
