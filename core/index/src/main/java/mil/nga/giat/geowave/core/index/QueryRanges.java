package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class QueryRanges
{
	private static class PartitionSortKeyRanges
	{
		private ByteArrayId partitionKey;
		private List<ByteArrayRange> sortKeyRanges;

		public PartitionSortKeyRanges(
				ByteArrayId partitionKey,
				List<ByteArrayRange> sortKeyRanges ) {
			this.partitionKey = partitionKey;
			this.sortKeyRanges = sortKeyRanges;
		}

		public PartitionSortKeyRanges(
				ByteArrayId partitionKey ) {
			this.partitionKey = partitionKey;
		}

		public PartitionSortKeyRanges(
				List<ByteArrayRange> sortKeyRanges ) {
			this.sortKeyRanges = sortKeyRanges;
		}
	}

	private final List<PartitionSortKeyRanges> partitions;
	private List<ByteArrayRange> compositeQueryRanges;

	public QueryRanges() {
		// this implies an infinite range
		partitions = null;
	}

	public QueryRanges(
			final List<ByteArrayId> partitionIds,
			final QueryRanges queryRanges ) {
		if (queryRanges2 == null){
			this.partitions
			
		}
		this.partitions = new ArrayList<>();
		for (PartitionSortKeyRanges sortKeyRange1 : queryRanges1.partitions){
				partitions.add(new PartitionSortKeyRanges(partitionKey));
			}
			for (PartitionSortKeyRanges sortKeyRange2 : queryRanges2.partitions){
				
			}
		}
	}

	public QueryRanges(
			List<PartitionSortKeyRanges> partitions ) {
		this.partitions = partitions;
	}

	public QueryRanges(
			List<ByteArrayId> partitionIds ) {
		this.partitions = Lists.transform(
				partitionIds,
				new Function<ByteArrayId, PartitionSortKeyRanges>() {
					@Override
					public PartitionSortKeyRanges apply(
							ByteArrayId input ) {
						return new PartitionSortKeyRanges(
								input);
					}
				});
	}

	public List<PartitionSortKeyRanges> getPartitions() {
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
		for (PartitionSortKeyRanges partition : partitions) {
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
