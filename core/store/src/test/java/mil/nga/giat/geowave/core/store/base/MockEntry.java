package mil.nga.giat.geowave.core.store.base;

import java.util.Random;
import java.util.UUID;

import mil.nga.giat.geowave.core.index.ByteArrayId;

public class MockEntry
{
	private final static long RANDOM_SEED = 1001l;
	private final static Random RANDOM_VALUES = new Random(
			RANDOM_SEED);
	private final double mockValue;
	private final String dataId;

	public MockEntry(
			final String dataId,
			final double mockValue ) {
		this.dataId = dataId;
		this.mockValue = mockValue;
	}

	public MockEntry() {
		dataId = UUID.randomUUID().toString();
		mockValue = RANDOM_VALUES.nextDouble();
	}

	public ByteArrayId getDataId() {
		return new ByteArrayId(
				dataId);
	}

	public double getMockValue() {
		return mockValue;
	}
}
