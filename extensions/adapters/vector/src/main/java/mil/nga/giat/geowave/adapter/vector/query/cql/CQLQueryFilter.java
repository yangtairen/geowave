package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;

import org.apache.log4j.Logger;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class CQLQueryFilter implements
		DistributableQueryFilter
{
	private final static Logger LOGGER = Logger.getLogger(CQLQueryFilter.class);
	private FeatureDataAdapter adapter;
	private Filter filter;

	protected CQLQueryFilter() {
		super();
	}

	public CQLQueryFilter(
			final Filter filter,
			final FeatureDataAdapter adapter ) {
		this.filter = filter;
		this.adapter = adapter;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding persistenceEncoding ) {
		if ((filter != null) && (indexModel != null) && (adapter != null)) {
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

				final SimpleFeature feature = adapter.decode(
						encoding,
						new Index(
								null, // because we know the feature data
										// adapter doesn't use the numeric index
										// strategy and only the common index
										// model to decode the simple feature,
										// we pass along a null strategy to
										// eliminate the necessity to send a
										// serialization of the strategy in the
										// options of this iterator
								indexModel));
				if (feature == null) {
					return false;
				}
				return filter.evaluate(feature);
			}
		}
		return true;
	}

	@Override
	public byte[] toBinary() {
		byte[] filterBytes;
		if (filter == null) {
			LOGGER.warn("CQL filter is null");
			filterBytes = new byte[] {};
		}
		else {
			filterBytes = StringUtils.stringToBinary(ECQL.toCQL(filter));
		}
		byte[] adapterBytes;
		if (adapter != null) {
			adapterBytes = PersistenceUtils.toBinary(adapter);
		}
		else {
			LOGGER.warn("Feature Data Adapter is null");
			adapterBytes = new byte[] {};
		}
		final ByteBuffer buf = ByteBuffer.allocate(filterBytes.length + adapterBytes.length + 4);
		buf.putInt(filterBytes.length);
		buf.put(filterBytes);
		buf.put(adapterBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int filterBytesLength = buf.getInt();
		final int adapterBytesLength = bytes.length - filterBytesLength - 4;
		if (filterBytesLength > 0) {
			final byte[] filterBytes = new byte[filterBytesLength];

			try {
				final String cql = StringUtils.stringFromBinary(filterBytes);
				filter = ECQL.toFilter(cql);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						e);
			}
		}
		else {
			LOGGER.warn("CQL filter is empty bytes");
			filter = null;
		}
		if (adapterBytesLength > 0) {
			final byte[] adapterBytes = new byte[adapterBytesLength];

			try {
				adapter = PersistenceUtils.fromBinary(
						adapterBytes,
						FeatureDataAdapter.class);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						e);
			}
		}
		else {
			LOGGER.warn("Feature Data Adapter is empty bytes");
			adapter = null;
		}
	}
}
