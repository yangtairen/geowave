package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class InsertionIds
{
	private final Collection<SinglePartitionInsertionIds> partitionKeys;
	private List<ByteArrayId> compositeInsertionIds;

	public InsertionIds(
			final List<ByteArrayId> sortKeys ) {
		this(
				new SinglePartitionInsertionIds(
						null,
						sortKeys));
	}

	public InsertionIds(
			final ByteArrayId partitionKey ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey));
	}

	public InsertionIds(
			final ByteArrayId partitionKey,
			final List<ByteArrayId> sortKeys ) {
		this(
				new SinglePartitionInsertionIds(
						partitionKey,
						sortKeys));
	}

	public InsertionIds(
			final SinglePartitionInsertionIds singePartitionKey ) {
		this(
				Arrays.asList(
						singePartitionKey));
	}

	public InsertionIds(
			final Collection<SinglePartitionInsertionIds> partitionKeys ) {
		this.partitionKeys = partitionKeys;
	}

	public Collection<SinglePartitionInsertionIds> getPartitionKeys() {
		return partitionKeys;
	}

	public List<ByteArrayId> getCompositeInsertionIds() {
		if (compositeInsertionIds != null) {
			return compositeInsertionIds;
		}
		if ((partitionKeys == null) || partitionKeys.isEmpty()) {
			return Collections.EMPTY_LIST;
		}
		final List<ByteArrayId> internalCompositeInsertionIds = new ArrayList<>();
		for (final SinglePartitionInsertionIds k : partitionKeys) {
			final List<ByteArrayId> i = k.getCompositeInsertionIds();
			if ((i != null) && !i.isEmpty()) {
				internalCompositeInsertionIds.addAll(
						i);
			}
		}
		compositeInsertionIds = internalCompositeInsertionIds;
		return compositeInsertionIds;
	}
}
