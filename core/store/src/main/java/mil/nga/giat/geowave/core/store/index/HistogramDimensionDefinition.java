package mil.nga.giat.geowave.core.store.index;

import java.nio.ByteBuffer;

import mil.nga.giat.geowave.core.index.dimension.BasicDimensionDefinition;
import mil.nga.giat.geowave.core.store.adapter.statistics.histogram.MinimalBinDistanceHistogram;

public class HistogramDimensionDefinition extends
		BasicDimensionDefinition
{
	private final MinimalBinDistanceHistogram histogram = new MinimalBinDistanceHistogram(
			4096);

	public HistogramDimensionDefinition() {
		super();
		// TODO Auto-generated constructor stub
	}

	public HistogramDimensionDefinition(
			double min,
			double max ) {
		super(
				min,
				max);
		// TODO Auto-generated constructor stub
	}

	public void add(
			final double value ) {
		histogram.add(
				value);
	}

	@Override
	public double normalize(
			final double value ) {
		return histogram.cdf(
				value);
	}

	@Override
	public double denormalize(
			final double value ) {
		return histogram.quantile(
				value);
	}

	@Override
	public byte[] toBinary() {
		final byte[] thisBinary = super.toBinary();
		final ByteBuffer buffer = ByteBuffer.allocate(
				histogram.bufferSize() + thisBinary.length);
		buffer.put(
				thisBinary);
		histogram.toBinary(
				buffer);
		return buffer.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buffer = ByteBuffer.wrap(
				bytes);
		final byte[] thisBinary = new byte[bytes.length - histogram.bufferSize()];
		buffer.get(thisBinary);
		super.fromBinary(
				thisBinary);
		histogram.fromBinary(
				buffer);
	}

}
