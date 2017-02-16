package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;

public class QueryRanges
{
	private static class PartitionSortKeyRanges
	{
		private ByteArrayId partitionKey;
		private List<ByteArrayRange> sortKeyRanges;

		public PartitionSortKeyRanges(
				final ByteArrayId partitionKey,
				final List<ByteArrayRange> sortKeyRanges ) {
			this.partitionKey = partitionKey;
			this.sortKeyRanges = sortKeyRanges;
		}

		public PartitionSortKeyRanges(
				final ByteArrayId partitionKey ) {
			this.partitionKey = partitionKey;
		}

		public PartitionSortKeyRanges(
				final List<ByteArrayRange> sortKeyRanges ) {
			this.sortKeyRanges = sortKeyRanges;
		}
	}

	private final Collection<PartitionSortKeyRanges> partitions;
	private List<ByteArrayRange> compositeQueryRanges;

	public QueryRanges() {
		// this implies an infinite range
		partitions = null;
	}

	public QueryRanges(
			final Set<ByteArrayId> partitionKeys,
			final QueryRanges queryRanges ) {
		if ((queryRanges == null) || (queryRanges.partitions == null) || queryRanges.partitions.isEmpty()) {
			partitions = fromPartitionKeys(
					partitionKeys);
		}
		else if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			partitions = queryRanges.partitions;
		}
		else {
			partitions = new ArrayList<>(
					partitionKeys.size() * queryRanges.partitions.size());
			for (final ByteArrayId partitionKey : partitionKeys) {
				for (final PartitionSortKeyRanges sortKeyRange : queryRanges.partitions) {
					ByteArrayId newPartitionKey;
					if (partitionKey == null) {
						newPartitionKey = sortKeyRange.partitionKey;
					}
					else if (sortKeyRange.partitionKey == null) {
						newPartitionKey = partitionKey;
					}
					else {
						newPartitionKey = new ByteArrayId(
								ByteArrayUtils.combineArrays(
										partitionKey.getBytes(),
										sortKeyRange.partitionKey.getBytes()));
					}
					partitions.add(
							new PartitionSortKeyRanges(
									newPartitionKey,
									sortKeyRange.sortKeyRanges));
				}
			}
		}
	}

	public QueryRanges(
			final Collection<PartitionSortKeyRanges> partitions ) {
		this.partitions = partitions;
	}

	public QueryRanges(
			final Set<ByteArrayId> partitionKeys ) {
		partitions = fromPartitionKeys(
				partitionKeys);
	}

	private static Collection<PartitionSortKeyRanges> fromPartitionKeys(
			final Set<ByteArrayId> partitionKeys ) {
		if (partitionKeys == null) {
			return null;
		}
		return Collections2.transform(
				partitionKeys,
				new Function<ByteArrayId, PartitionSortKeyRanges>() {
					@Override
					public PartitionSortKeyRanges apply(
							ByteArrayId input ) {
						return new PartitionSortKeyRanges(
								input);
					}
				});
	}

	public Collection<PartitionSortKeyRanges> getPartitions() {
		return partitions;
	}

	public List<ByteArrayRange> getCompositeQueryRanges() {
		if (partitions == null) {
			return null;
		}
		if (compositeQueryRanges != null) {
			return compositeQueryRanges;
		}
		if (partitions.isEmpty()) {
			compositeQueryRanges = new ArrayList<>();
			return compositeQueryRanges;
		}
		final List<ByteArrayRange> internalQueryRanges = new ArrayList<>();
		for (final PartitionSortKeyRanges partition : partitions) {
			if ((partition.sortKeyRanges == null) || partition.sortKeyRanges.isEmpty()) {
				internalQueryRanges.add(
						new ByteArrayRange(
								partition.partitionKey,
								partition.partitionKey,
								true));
			}

			else if (partition.partitionKey == null) {
				internalQueryRanges.addAll(
						partition.sortKeyRanges);
			}
			else {
				for (final ByteArrayRange sortKeyRange : partition.sortKeyRanges) {
					internalQueryRanges.add(
							new ByteArrayRange(
									new ByteArrayId(
											ByteArrayUtils.combineArrays(
													partition.partitionKey.getBytes(),
													sortKeyRange.getStart().getBytes())),
									new ByteArrayId(
											ByteArrayUtils.combineArrays(
													partition.partitionKey.getBytes(),
													sortKeyRange.getEnd().getBytes())),
									sortKeyRange.singleValue));
				}
			}
		}

		compositeQueryRanges = internalQueryRanges;
		return compositeQueryRanges;
	}
}
