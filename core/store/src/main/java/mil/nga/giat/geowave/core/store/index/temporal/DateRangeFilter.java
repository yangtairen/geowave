package mil.nga.giat.geowave.core.store.index.temporal;

import java.nio.ByteBuffer;
import java.util.Date;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.lexicoder.Lexicoders;
import mil.nga.giat.geowave.core.store.data.IndexedPersistenceEncoding;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

public class DateRangeFilter implements
		DistributableQueryFilter
{
	protected ByteArrayId fieldId;
	protected Date start;
	protected Date end;
	protected boolean rangeInclusive;

	protected DateRangeFilter() {
		super();
	}

	public DateRangeFilter(
			final ByteArrayId fieldId,
			final Date start,
			final Date end,
			final boolean rangeInclusive ) {
		super();
		this.fieldId = fieldId;
		this.start = start;
		this.end = end;
		this.rangeInclusive = rangeInclusive;
	}

	@Override
	public boolean accept(
			final CommonIndexModel indexModel,
			final IndexedPersistenceEncoding<?> persistenceEncoding ) {
		final ByteArrayId dateLongBytes = (ByteArrayId) persistenceEncoding.getCommonData().getValue(
				fieldId);
		if (dateLongBytes != null) {
			final long dateLong = Lexicoders.LONG.fromByteArray(dateLongBytes.getBytes());
			final Date value = new Date(
					dateLong);
			if (start != null) {
				if (rangeInclusive) {
					if (value.compareTo(start) < 0) {
						return false;
					}
					else if (value.compareTo(start) <= 0) {
						return false;
					}
				}
			}
			if (end != null) {
				if (rangeInclusive) {
					if (value.compareTo(end) > 0) {
						return false;
					}
					else if (value.compareTo(end) >= 0) {
						return false;
					}
				}
			}
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public byte[] toBinary() {
		final ByteBuffer bb = ByteBuffer.allocate(4 + fieldId.getBytes().length + 8 + 8 + 4);
		bb.putInt(fieldId.getBytes().length);
		bb.put(fieldId.getBytes());
		bb.putLong(start.getTime());
		bb.putLong(end.getTime());
		final int rangeInclusiveInt = (rangeInclusive) ? 1 : 0;
		bb.putInt(rangeInclusiveInt);
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
		start = new Date(
				bb.getLong());
		end = new Date(
				bb.getLong());
		rangeInclusive = (bb.getInt() == 1) ? true : false;
	}

}
