package mil.nga.giat.geowave.core.store.index.numeric;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class EqualsFilter implements
		DistributableQueryFilter
{
	protected ByteArrayId fieldId;
	protected Number number;

	protected EqualsFilter() {
		super();
	}

	public EqualsFilter(
			final ByteArrayId fieldId,
			final Number number ) {
		super();
		this.fieldId = fieldId;
		this.number = number;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArrayId value = (ByteArrayId) persistenceEncoding.getCommonData().getValue(
				fieldId);
		if (value != null) {
			final double val = Lexicoders.DOUBLE.fromByteArray(value.getBytes());
			return val == number.doubleValue();
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer bb = ByteBuffer.allocate(4 + fieldId.getBytes().length + 8);
		bb.putInt(fieldId.getBytes().length);
		bb.put(fieldId.getBytes());
		bb.putDouble(number.doubleValue());
		return bb.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer bb = ByteBuffer.wrap(bytes);
		final byte[] fieldIdBytes = new byte[bb.getInt()];
		bb.get(fieldIdBytes);
		fieldId = new ByteArrayId(
				fieldIdBytes);
		number = new Double(
				bb.getDouble());
	}

}
