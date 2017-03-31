package mil.nga.giat.geowave.core.store.entities;

import java.nio.ByteBuffer;

public class GeoWaveKeyImpl implements
		GeoWaveKey
{
	protected byte[] dataId = null;
	protected byte[] adapterId = null;
	protected byte[] partitionKey = null;
	protected byte[] sortKey = null;
	protected int numberOfDuplicates = 0;

	protected GeoWaveKeyImpl() {}

	public GeoWaveKeyImpl(
			final byte[] compositeInsertionId,
			final int partitionKeyLength ) {
		this(
				compositeInsertionId,
				partitionKeyLength,
				compositeInsertionId.length);
	}

	public GeoWaveKeyImpl(
			final byte[] compositeInsertionId,
			final int partitionKeyLength,
			final int length ) {
		this(
				compositeInsertionId,
				partitionKeyLength,
				0,
				length);
	}

	public GeoWaveKeyImpl(
			final byte[] compositeInsertionId,
			final int partitionKeyLength,
			final int offset,
			final int length ) {
		final ByteBuffer metadataBuf = ByteBuffer.wrap(
				compositeInsertionId,
				(length + offset) - 12,
				12);
		final int adapterIdLength = metadataBuf.getInt();
		final int dataIdLength = metadataBuf.getInt();
		final int numberOfDuplicates = metadataBuf.getInt();

		final ByteBuffer buf = ByteBuffer.wrap(
				compositeInsertionId,
				offset,
				length - 12);
		final byte[] sortKey = new byte[length - 12 - adapterIdLength - dataIdLength - partitionKeyLength];
		final byte[] partitionKey = new byte[length - 12 - adapterIdLength - dataIdLength - sortKey.length];
		final byte[] adapterId = new byte[adapterIdLength];
		final byte[] dataId = new byte[dataIdLength];
		buf.get(
				partitionKey);
		buf.get(
				sortKey);
		buf.get(
				adapterId);
		buf.get(
				dataId);

		this.dataId = dataId;
		this.adapterId = adapterId;
		this.partitionKey = partitionKey;
		this.sortKey = sortKey;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	public GeoWaveKeyImpl(
			final byte[] dataId,
			final byte[] adapterId,
			final byte[] partitionKey,
			final byte[] sortKey,
			final int numberOfDuplicates ) {
		this.dataId = dataId;
		this.adapterId = adapterId;
		this.partitionKey = partitionKey;
		this.sortKey = sortKey;
		this.numberOfDuplicates = numberOfDuplicates;
	}

	@Override
	public byte[] getDataId() {
		return dataId;
	}

	@Override
	public byte[] getAdapterId() {
		return adapterId;
	}

	@Override
	public byte[] getPartitionKey() {
		return partitionKey;
	}

	@Override
	public byte[] getSortKey() {
		return sortKey;
	}

	@Override
	public int getNumberOfDuplicates() {
		return numberOfDuplicates;
	}

	public boolean isDeduplicationEnabled() {
		return numberOfDuplicates >= 0;
	}
}
