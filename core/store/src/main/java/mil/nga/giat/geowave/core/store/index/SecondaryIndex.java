package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;

import com.google.common.base.Joiner;

/**
 * This class fully describes everything necessary to index data within GeoWave.
 * The key components are the indexing strategy and the common index model.
 */
public class SecondaryIndex<T> implements
		Index<FilterableConstraints, List<FieldInfo<?>>>
{
	private FieldIndexStrategy<?, ?> indexStrategy;
	private ByteArrayId[] fieldIDs;

	public SecondaryIndex(
			final FieldIndexStrategy<?, ?> indexStrategy,
			final ByteArrayId[] fieldIDs ) {
		this.indexStrategy = indexStrategy;
		this.fieldIDs = fieldIDs;
	}

	public SecondaryIndex(
			final FieldIndexStrategy<?, ?> indexStrategy,
			final ByteArrayId[] fieldIDs,
			final List<DataStatistics<T>> associatedStatistics ) {
		super();
		this.indexStrategy = indexStrategy;
		this.fieldIDs = fieldIDs;
	}

	@SuppressWarnings({
		"unchecked",
		"rawtypes"
	})
	@Override
	public FieldIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public ByteArrayId[] getFieldIDs() {
		return fieldIDs;
	}

	@Override
	public ByteArrayId getId() {
		return new ByteArrayId(
				StringUtils.stringToBinary(indexStrategy.getId() + "#" + Joiner.on(
						"#").join(
						fieldIDs)));
	}

	@Override
	public int hashCode() {
		return getId().hashCode();
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final SecondaryIndex<?> other = (SecondaryIndex<?>) obj;
		return getId().equals(
				other.getId());
	}

	@Override
	public byte[] toBinary() {
		final byte[] indexStrategyBinary = PersistenceUtils.toBinary(indexStrategy);
		final byte[] fieldIdBinary = ByteArrayId.toBytes(fieldIDs);
		final ByteBuffer buf = ByteBuffer.allocate(indexStrategyBinary.length + fieldIdBinary.length + 8);
		buf.putInt(indexStrategyBinary.length);
		buf.putInt(fieldIdBinary.length);
		buf.put(indexStrategyBinary);
		buf.put(fieldIdBinary);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int indexStrategyLength = buf.getInt();
		final int fieldIdLength = buf.getInt();
		final byte[] indexStrategyBinary = new byte[indexStrategyLength];
		final byte[] fieldIdBinary = new byte[fieldIdLength];
		buf.get(indexStrategyBinary);
		buf.get(fieldIdBinary);

		indexStrategy = PersistenceUtils.fromBinary(
				indexStrategyBinary,
				FieldIndexStrategy.class);

		fieldIDs = ByteArrayId.fromBytes(fieldIdBinary);
	}

}
