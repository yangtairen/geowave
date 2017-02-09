package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertionIds
{
	private List<ByteArrayId> compositeInsertionIds;
	private final ByteArrayId partitionKey;
	private List<ByteArrayId> sortKeys;

	public InsertionIds(
			final InsertionIds insertionId1,
			final InsertionIds insertionId2 ) {
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

	public InsertionIds(
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

}
