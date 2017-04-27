package mil.nga.giat.geowave.core.store.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import com.google.common.primitives.UnsignedBytes;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.Deleter;
import mil.nga.giat.geowave.core.store.base.Reader;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public class MemoryDataStoreOperations implements
		DataStoreOperations
{
	private final static Logger LOGGER = Logger.getLogger(
			MemoryDataStoreOperations.class);
	private final Map<ByteArrayId, TreeSet<MemoryStoreEntry>> storeData = Collections.synchronizedMap(
			new HashMap<ByteArrayId, TreeSet<MemoryStoreEntry>>());

	public MemoryDataStoreOperations() {}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException {
		return storeData.containsKey(
				indexId);
	}

	@Override
	public void deleteAll()
			throws Exception {
		storeData.clear();
	}

	@Override
	public boolean deleteAll(
			final ByteArrayId tableName,
			final ByteArrayId adapterId,
			final String... additionalAuthorizations ) {
		return false;
	}

	@Override
	public boolean insureAuthorizations(
			final String clientUser,
			final String... authorizations ) {
		return true;
	}

	@Override
	public Writer createWriter(
			final ByteArrayId indexId,
			final ByteArrayId adapterId,
			final DataStoreOptions options,
			final Set<ByteArrayId> splits ) {
		return new MyIndexWriter<>(
				indexId);
	}

	@Override
	public Deleter<? extends GeoWaveRow> createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception {
		return new MyIndexDeleter(
				indexId,
				authorizations);
	}

	protected TreeSet<MemoryStoreEntry> getRowsForIndex(
			final ByteArrayId id ) {
		TreeSet<MemoryStoreEntry> set = storeData.get(
				id);
		if (set == null) {
			set = new TreeSet<MemoryStoreEntry>();
			storeData.put(
					id,
					set);
		}
		return set;
	}

	@Override
	public Reader createReader(
			final PrimaryIndex index,
			final List<ByteArrayId> adapterIds,
			final double[] maxResolutionSubsamplingPerDimension,
			final Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			final Pair<List<String>, DataAdapter<?>> fieldSubsets,
			final boolean isWholeRow,
			final QueryRanges ranges,
			final DistributableQueryFilter filter,
			final Integer limit,
			final String... additionalAuthorizations ) {
		final TreeSet<MemoryStoreEntry> internalData = storeData.get(
				index.getId());
		int counter = 0;
		List<MemoryStoreEntry> retVal = new ArrayList<>();
		final Collection<SinglePartitionQueryRanges> partitionRanges = ranges.getPartitionQueryRanges();
		if ((partitionRanges == null) || partitionRanges.isEmpty()) {
			retVal.addAll(
					internalData);
			// remove unauthorized
			final Iterator<MemoryStoreEntry> it = retVal.iterator();
			while (it.hasNext()) {
				if (!isAuthorized(
						it.next(),
						additionalAuthorizations)) {
					it.remove();
				}
			}
			if ((limit != null) && (limit > 0) && (retVal.size() > limit)) {
				retVal = retVal.subList(
						0,
						limit);
			}
		}
		else {
			for (final SinglePartitionQueryRanges p : partitionRanges) {
				for (final ByteArrayRange r : p.getSortKeyRanges()) {
					final SortedSet<MemoryStoreEntry> set;
					if (r.isSingleValue()) {
						set = internalData.subSet(
								new MemoryStoreEntry(
										p.getPartitionKey(),
										r.getStart()),
								new MemoryStoreEntry(
										p.getPartitionKey(),
										new ByteArrayId(
												r.getStart().getNextPrefix())));
					}
					else {
						set = internalData.tailSet(
								new MemoryStoreEntry(
										p.getPartitionKey(),
										r.getStart()),
								true).headSet(
										new MemoryStoreEntry(
												p.getPartitionKey(),
												r.getEndAsNextPrefix()));
					}
					// remove unauthorized
					final Iterator<MemoryStoreEntry> it = set.iterator();
					while (it.hasNext()) {
						if (!isAuthorized(
								it.next(),
								additionalAuthorizations)) {
							it.remove();
						}
					}
					if ((limit != null) && (limit > 0) && ((counter + set.size()) > limit)) {
						final List<MemoryStoreEntry> subset = new ArrayList<>(
								set);
						retVal.addAll(
								subset.subList(
										0,
										limit - counter));
						break;
					}
					else {
						retVal.addAll(
								set);
						counter += set.size();
						if (limit > 0 && counter >= limit) {
							break;
						}
					}
				}
			}
		}
		return new MyIndexReader(
				retVal.iterator());
	}

	private boolean isAuthorized(
			final MemoryStoreEntry row,
			final String... authorizations ) {
		for (final GeoWaveValue value : row.getRow().getFieldValues()) {
			if (!MemoryStoreUtils.isAuthorized(
					value.getVisibility(),
					authorizations)) {
				return false;
			}
		}
		return true;
	}

	private class MyIndexReader implements
			Reader
	{
		private final Iterator<MemoryStoreEntry> it;

		public MyIndexReader(
				final Iterator<MemoryStoreEntry> it ) {
			super();
			this.it = it;
		}

		@Override
		public void close()
				throws Exception {}

		@Override
		public boolean hasNext() {
			return it.hasNext();
		}

		@Override
		public GeoWaveRow next() {
			return it.next().row;
		}
	}

	private class MyIndexWriter<T> implements
			Writer
	{
		final ByteArrayId indexId;

		public MyIndexWriter(
				final ByteArrayId indexId ) {
			super();
			this.indexId = indexId;
		}

		@Override
		public void close()
				throws IOException {}

		@Override
		public void flush() {
			try {
				close();
			}
			catch (final IOException e) {
				LOGGER.error(
						"Error closing index writer",
						e);
			}
		}

		@Override
		public void write(
				final GeoWaveRow[] rows ) {
			for (final GeoWaveRow r : rows) {
				write(
						r);
			}
		}

		@Override
		public void write(
				final GeoWaveRow row ) {
			TreeSet<MemoryStoreEntry> rowTreeSet = storeData.get(
					indexId);
			if (rowTreeSet == null) {
				rowTreeSet = new TreeSet<>();
				storeData.put(
						indexId,
						rowTreeSet);
			}
			if (rowTreeSet.contains(
					new MemoryStoreEntry(
							row))) {
				rowTreeSet.remove(
						new MemoryStoreEntry(
								row));
			}
			if (!rowTreeSet.add(
					new MemoryStoreEntry(
							row))) {
				LOGGER.warn(
						"Unable to add new entry");
			}
		}
	}

	private class MyIndexDeleter implements
			Deleter<GeoWaveRowImpl>
	{
		private final ByteArrayId indexId;
		private final String[] authorizations;

		public MyIndexDeleter(
				final ByteArrayId indexId,
				final String... authorizations ) {
			this.indexId = indexId;
			this.authorizations = authorizations;
		}

		@Override
		public void close()
				throws Exception {}

		@Override
		public void delete(
				final GeoWaveRowImpl row,
				final DataAdapter<?> adapter ) {
			final MemoryStoreEntry entry = new MemoryStoreEntry(
					row);
			if (isAuthorized(
					entry,
					authorizations)) {
				final TreeSet<MemoryStoreEntry> rowTreeSet = storeData.get(
						indexId);
				if (rowTreeSet != null) {
					if (!rowTreeSet.remove(
							entry)) {
						LOGGER.warn(
								"Unable to remove entry");
					}
				}
			}
		}
	}

	public static class MemoryStoreEntry implements
			Comparable<MemoryStoreEntry>
	{
		private final GeoWaveRow row;

		public MemoryStoreEntry(
				final ByteArrayId comparisonPartitionKey,
				final ByteArrayId comparisonSortKey ) {
			row = new GeoWaveRowImpl(
					new GeoWaveKeyImpl(
							new byte[] {
								0
							},
							new byte[] {},
							comparisonPartitionKey.getBytes(),
							comparisonSortKey.getBytes(),
							0),
					null);
		}

		public MemoryStoreEntry(
				final GeoWaveRow row ) {
			this.row = row;
		}

		public GeoWaveRow getRow() {
			return row;
		}

		public byte[] getCompositeInsertionId() {
			return ((GeoWaveKeyImpl) ((GeoWaveRowImpl) row).getKey()).getCompositeInsertionId();
		}

		@Override
		public int compareTo(
				final MemoryStoreEntry other ) {
			final int indexIdCompare = UnsignedBytes.lexicographicalComparator().compare(
					getCompositeInsertionId(),
					other.getCompositeInsertionId());
			if (indexIdCompare != 0) {
				return indexIdCompare;
			}
			final int dataIdCompare = UnsignedBytes.lexicographicalComparator().compare(
					row.getDataId(),
					other.getRow().getDataId());
			if (dataIdCompare != 0) {
				return dataIdCompare;
			}
			final int adapterIdCompare = UnsignedBytes.lexicographicalComparator().compare(
					row.getAdapterId(),
					other.getRow().getAdapterId());
			if (adapterIdCompare != 0) {
				return adapterIdCompare;
			}
			return 0;

		}
	}
}
