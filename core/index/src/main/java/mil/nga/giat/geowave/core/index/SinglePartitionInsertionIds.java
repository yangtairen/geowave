package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SinglePartitionInsertionIds
{
	private List<ByteArrayId> compositeInsertionIds;
	private final ByteArrayId partitionKey;
	private List<ByteArrayId> sortKeys;

	public SinglePartitionInsertionIds(
			final ByteArrayId partitionKey ) {
		this(
				partitionKey,
				(ByteArrayId) null);
	}

	public SinglePartitionInsertionIds(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		this.partitionKey = partitionKey;
		sortKeys = sortKey == null ? null : Arrays.asList(
				sortKey);
	}

	public SinglePartitionInsertionIds(
			final ByteArrayId partitionKey,
			final SinglePartitionInsertionIds insertionId2 ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey,
						(List<ByteArrayId>) null),
				insertionId2);
	}

	public SinglePartitionInsertionIds(
			final SinglePartitionInsertionIds insertionId1,
			final SinglePartitionInsertionIds insertionId2 ) {
		partitionKey = new ByteArrayId(
				ByteArrayUtils.combineArrays(
						insertionId1.partitionKey.getBytes(),
						insertionId2.partitionKey.getBytes()));
		if ((insertionId1.sortKeys == null) || insertionId1.sortKeys.isEmpty()) {
			sortKeys = insertionId2.sortKeys;
		}
		else if ((insertionId2.sortKeys == null) || insertionId2.sortKeys.isEmpty()) {
			sortKeys = insertionId1.sortKeys;
		}
		else {
			// use all permutations of range keys
			sortKeys = new ArrayList<ByteArrayId>(
					insertionId1.sortKeys.size() * insertionId2.sortKeys.size());
			for (final ByteArrayId sortKey1 : insertionId1.sortKeys) {
				for (final ByteArrayId sortKey2 : insertionId2.sortKeys) {
					sortKeys.add(
							new ByteArrayId(
									ByteArrayUtils.combineArrays(
											sortKey1.getBytes(),
											sortKey2.getBytes())));
				}
			}
		}
	}

	public SinglePartitionInsertionIds(
			final ByteArrayId partitionKey,
			final List<ByteArrayId> sortKeys ) {
		this.partitionKey = partitionKey;
		this.sortKeys = sortKeys;
	}

	public List<ByteArrayId> getCompositeInsertionIds() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds;
		}

		if ((sortKeys == null) || sortKeys.isEmpty()) {
			compositeInsertionIds = Arrays.asList(
					partitionKey);
			return compositeInsertionIds;
		}

		if (partitionKey == null) {
			compositeInsertionIds = sortKeys;
			return compositeInsertionIds;
		}

		final List<ByteArrayId> internalInsertionIds = new ArrayList<>(
				sortKeys.size());
		for (final ByteArrayId sortKey : sortKeys) {
			internalInsertionIds.add(
					new ByteArrayId(
							ByteArrayUtils.combineArrays(
									partitionKey.getBytes(),
									sortKey.getBytes())));
		}
		compositeInsertionIds = internalInsertionIds;
		return compositeInsertionIds;
	}

	public ByteArrayId getPartitionKey() {
		return partitionKey;
	}

	public List<ByteArrayId> getSortKeys() {
		return sortKeys;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + ((partitionKey == null) ? 0 : partitionKey.hashCode());
		result = (prime * result) + ((sortKeys == null) ? 0 : sortKeys.hashCode());
		return result;
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
		final SinglePartitionInsertionIds other = (SinglePartitionInsertionIds) obj;
		if (partitionKey == null) {
			if (other.partitionKey != null) {
				return false;
			}
		}
		else if (!partitionKey.equals(
				other.partitionKey)) {
			return false;
		}
		if (sortKeys == null) {
			if (other.sortKeys != null) {
				return false;
			}
		}
		else if (!sortKeys.equals(
				other.sortKeys)) {
			return false;
		}
		return true;
	}

}
