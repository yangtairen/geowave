package mil.nga.giat.geowave.core.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class InsertionIds implements
		Persistable
{
	private Collection<SinglePartitionInsertionIds> partitionKeys;
	private List<ByteArrayId> compositeInsertionIds;

	public InsertionIds() {}

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

	public boolean contains(
			final ByteArrayId partitionKey,
			final ByteArrayId sortKey ) {
		for (final SinglePartitionInsertionIds p : partitionKeys) {
			if (((partitionKey == null) && (p.getPartitionKey() == null))
					|| ((partitionKey != null) && partitionKey.equals(
							p.getPartitionKey()))) {
				// partition key matches find out if sort key is contained
				if (sortKey == null) {
					return true;
				}
				if ((p.getSortKeys() != null) && p.getSortKeys().contains(
						sortKey)) {
					return true;
				}
				return false;
			}
		}
		return false;
	}

	@Override
	public byte[] toBinary() {
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}
}
