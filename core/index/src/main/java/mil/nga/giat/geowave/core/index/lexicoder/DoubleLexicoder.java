package mil.nga.giat.geowave.core.index.lexicoder;

import com.google.common.base.Preconditions;

/**
 * A lexicoder for preserving the native Java sort order of Double values.
 * 
 * See Apache Accumulo:
 * org.apache.accumulo.core.client.lexicoder.DoubleLexicoder
 * 
 */
public class DoubleLexicoder implements
		NumberLexicoder<Double>
{

	@Override
	public byte[] toByteArray(
			final Double value ) {
		long l = Double.doubleToRawLongBits(value);
		if (l < 0) {
			l = ~l;
		}
		else {
			l = l ^ 0x8000000000000000l;
		}
		return encode(l);
	}

	@Override
	public Double fromByteArray(
			final byte[] bytes ) {
		Preconditions.checkNotNull(
				bytes,
				"cannot decode null byte array");
		return decode(
				bytes,
				0,
				bytes.length);
	}

	@Override
	public Double getMinimumValue() {
		return (double) Long.MIN_VALUE;
	}

	@Override
	public Double getMaximumValue() {
		return Double.MAX_VALUE;
	}

	private byte[] encode(
			final Long l ) {
		int shift = 56;
		int index;
		final int prefix = l < 0 ? 0xff : 0x00;
		for (index = 0; index < 8; index++) {
			if (((l >>> shift) & 0xff) != prefix) {
				break;
			}
			shift -= 8;
		}
		final byte ret[] = new byte[9 - index];
		ret[0] = (byte) (8 - index);
		for (index = 1; index < ret.length; index++) {
			ret[index] = (byte) (l >>> shift);
			shift -= 8;
		}
		if (l < 0) {
			ret[0] = (byte) (16 - ret[0]);
		}
		return ret;
	}

	private Double decode(
			final byte[] data,
			final int offset,
			final int len ) {
		long l = 0;
		int shift = 0;
		if ((data[offset] < 0) || (data[offset] > 16)) {
			throw new IllegalArgumentException(
					"Unexpected length " + (0xff & data[offset]));
		}
		for (int i = (offset + len) - 1; i >= (offset + 1); i--) {
			l += (data[i] & 0xffl) << shift;
			shift += 8;
		}
		// fill in 0xff prefix
		if (data[offset] > 8) {
			l |= -1l << ((16 - data[offset]) << 3);
		}
		if (l < 0) {
			l = l ^ 0x8000000000000000l;
		}
		else {
			l = ~l;
		}
		return Double.longBitsToDouble(l);
	}

}
