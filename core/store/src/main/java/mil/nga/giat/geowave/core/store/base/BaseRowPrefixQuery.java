package mil.nga.giat.geowave.core.store.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.visibility.DifferingFieldVisibilityEntryCount;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/**
 * Represents a query operation using an Accumulo row prefix.
 *
 */
class BaseRowPrefixQuery<T> extends
		AbstractBaseRowQuery<T>
{

	final Integer limit;
	final QueryRanges queryRanges;

	public BaseRowPrefixQuery(
			final BaseDataStore dataStore,
			final PrimaryIndex index,
			final ByteArrayId rowPrefix,
			final ScanCallback<T, ?> scanCallback,
			final Integer limit,
			final DifferingFieldVisibilityEntryCount visibilityCounts,
			final String[] authorizations ) {
		super(
				dataStore,
				index,
				authorizations,
				scanCallback,
				visibilityCounts);
		this.limit = limit;
		final Set<ByteArrayId> partitions = index.getIndexStrategy().getPartitionKeys();

		final ByteArrayRange rowPrefixRange = new ByteArrayRange(
				rowPrefix,
				rowPrefix,
				false);
		if ((partitions == null) || partitions.isEmpty()) {
			queryRanges = new QueryRanges(
					rowPrefixRange);
		}
		else {
			final List<SinglePartitionQueryRanges> ranges = new ArrayList<SinglePartitionQueryRanges>();
			final Collection<ByteArrayRange> sortKeys = Collections.singleton(
					rowPrefixRange);
			for (final ByteArrayId partitionKey : partitions) {
				ranges.add(
						new SinglePartitionQueryRanges(
								partitionKey,
								sortKeys));
			}
			queryRanges = new QueryRanges(
					ranges);
		}
	}

	@Override
	protected Integer getScannerLimit() {
		return limit;
	}

	@Override
	protected QueryRanges getRanges() {
		return queryRanges;
	}

}
