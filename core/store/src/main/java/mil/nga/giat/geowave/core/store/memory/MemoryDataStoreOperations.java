package mil.nga.giat.geowave.core.store.memory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.primitives.UnsignedBytes;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.SinglePartitionQueryRanges;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKeyImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveMetadata;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRowImpl;
import mil.nga.giat.geowave.core.store.entities.GeoWaveValue;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;
import mil.nga.giat.geowave.core.store.operations.Deleter;
import mil.nga.giat.geowave.core.store.operations.MetadataDeleter;
import mil.nga.giat.geowave.core.store.operations.MetadataQuery;
import mil.nga.giat.geowave.core.store.operations.MetadataReader;
import mil.nga.giat.geowave.core.store.operations.MetadataType;
import mil.nga.giat.geowave.core.store.operations.MetadataWriter;
import mil.nga.giat.geowave.core.store.operations.Reader;
import mil.nga.giat.geowave.core.store.operations.ReaderParams;
import mil.nga.giat.geowave.core.store.operations.Writer;

public class MemoryDataStoreOperations implements
		DataStoreOperations
{
	private final static Logger LOGGER = Logger.getLogger(MemoryDataStoreOperations.class);
	private final Map<ByteArrayId, TreeSet<MemoryStoreEntry>> storeData = Collections
			.synchronizedMap(new HashMap<ByteArrayId, TreeSet<MemoryStoreEntry>>());
	private final Map<MetadataType, TreeSet<MemoryMetadataEntry>> metadataStore = Collections
			.synchronizedMap(new HashMap<MetadataType, TreeSet<MemoryMetadataEntry>>());

	public MemoryDataStoreOperations() {}

	@Override
	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException {
		return storeData.containsKey(indexId);
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
			final Set<ByteArrayId> splits ) {
		return new MyIndexWriter<>(
				indexId);
	}

	@Override
	public Deleter createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception {
		return new MyIndexDeleter(
				indexId,
				authorizations);
	}

	protected TreeSet<MemoryStoreEntry> getRowsForIndex(
			final ByteArrayId id ) {
		TreeSet<MemoryStoreEntry> set = storeData.get(id);
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
			final ReaderParams readerParams ) {
		final TreeSet<MemoryStoreEntry> internalData = storeData.get(readerParams.getIndex().getId());
		int counter = 0;
		List<MemoryStoreEntry> retVal = new ArrayList<>();
		final Collection<SinglePartitionQueryRanges> partitionRanges = readerParams
				.getQueryRanges()
				.getPartitionQueryRanges();
		if ((partitionRanges == null) || partitionRanges.isEmpty()) {
			retVal.addAll(internalData);
			// remove unauthorized
			final Iterator<MemoryStoreEntry> it = retVal.iterator();
			while (it.hasNext()) {
				if (!isAuthorized(
						it.next(),
						readerParams.getAdditionalAuthorizations())) {
					it.remove();
				}
			}
			if ((readerParams.getLimit() != null) && (readerParams.getLimit() > 0)
					&& (retVal.size() > readerParams.getLimit())) {
				retVal = retVal.subList(
						0,
						readerParams.getLimit());
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
								readerParams.getAdditionalAuthorizations())) {
							it.remove();
						}
					}
					if ((readerParams.getLimit() != null) && (readerParams.getLimit() > 0)
							&& ((counter + set.size()) > readerParams.getLimit())) {
						final List<MemoryStoreEntry> subset = new ArrayList<>(
								set);
						retVal.addAll(subset.subList(
								0,
								readerParams.getLimit() - counter));
						break;
					}
					else {
						retVal.addAll(set);
						counter += set.size();
						if ((readerParams.getLimit() > 0) && (counter >= readerParams.getLimit())) {
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
				write(r);
			}
		}

		@Override
		public void write(
				final GeoWaveRow row ) {
			TreeSet<MemoryStoreEntry> rowTreeSet = storeData.get(indexId);
			if (rowTreeSet == null) {
				rowTreeSet = new TreeSet<>();
				storeData.put(
						indexId,
						rowTreeSet);
			}
			if (rowTreeSet.contains(new MemoryStoreEntry(
					row))) {
				rowTreeSet.remove(new MemoryStoreEntry(
						row));
			}
			if (!rowTreeSet.add(new MemoryStoreEntry(
					row))) {
				LOGGER.warn("Unable to add new entry");
			}
		}
	}

	private class MyIndexDeleter implements
			Deleter
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
				final GeoWaveRow row,
				final DataAdapter<?> adapter ) {
			final MemoryStoreEntry entry = new MemoryStoreEntry(
					row);
			if (isAuthorized(
					entry,
					authorizations)) {
				final TreeSet<MemoryStoreEntry> rowTreeSet = storeData.get(indexId);
				if (rowTreeSet != null) {
					if (!rowTreeSet.remove(entry)) {
						LOGGER.warn("Unable to remove entry");
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

	@Override
	public MetadataWriter createMetadataWriter(
			final MetadataType metadataType ) {
		return new MyMetadataWriter<>(
				metadataType);
	}

	@Override
	public MetadataReader createMetadataReader(
			final MetadataType metadataType ) {
		return new MyMetadataReader(
				metadataType);
	}

	@Override
	public MetadataDeleter createMetadataDeleter(
			final MetadataType metadataType ) {
		return new MyMetadataDeleter(
				metadataType);
	}

	private class MyMetadataReader implements
			MetadataReader
	{
		private final MetadataType type;

		public MyMetadataReader(
				final MetadataType type ) {
			super();
			this.type = type;
		}

		@Override
		public CloseableIterator<GeoWaveMetadata> query(
				final MetadataQuery query ) {
			final TreeSet<MemoryMetadataEntry> typeStore = metadataStore.get(type);
			if (typeStore == null) {
				return new CloseableIterator.Empty<GeoWaveMetadata>();
			}
			final SortedSet<MemoryMetadataEntry> set = typeStore.subSet(
					new MemoryMetadataEntry(
							new GeoWaveMetadata(
									query.getPrimaryId(),
									query.getSecondaryId(),
									null,
									null)),
					new MemoryMetadataEntry(
							new GeoWaveMetadata(
									getNextPrefix(query.getPrimaryId()),
									getNextPrefix(query.getSecondaryId()),
									// this should be sufficient
									new byte[] {
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF
									},
									// this should be sufficient
									new byte[] {
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF,
										(byte) 0xFF
									})));
			Iterator<MemoryMetadataEntry> it = set.iterator();
			if ((query.getAuthorizations() != null) && (query.getAuthorizations().length > 0)) {
				it = Iterators.filter(
						it,
						new Predicate<MemoryMetadataEntry>() {
							@Override
							public boolean apply(
									final MemoryMetadataEntry input ) {
								return MemoryStoreUtils.isAuthorized(
										input.getMetadata().getVisibility(),
										query.getAuthorizations());
							}
						});
			}
			return new CloseableIterator.Wrapper(
					it);
		}

	}

	private static byte[] getNextPrefix(
			final byte[] bytes ) {
		if (bytes == null) {
			// TODO investigate if this is the right thing to do, its the
			// equivalent of the last position from getNextPrefix() but it
			// seems wrong
			return new byte[0];
		}
		return new ByteArrayId(
				bytes).getNextPrefix();
	}

	private class MyMetadataWriter<T> implements
			MetadataWriter
	{
		private final MetadataType type;

		public MyMetadataWriter(
				final MetadataType type ) {
			super();
			this.type = type;
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
						"Error closing metadata writer",
						e);
			}
		}

		@Override
		public void write(
				final GeoWaveMetadata metadata ) {

			TreeSet<MemoryMetadataEntry> typeStore = metadataStore.get(type);
			if (typeStore == null) {
				typeStore = new TreeSet<>();
				metadataStore.put(
						type,
						typeStore);
			}
			if (typeStore.contains(new MemoryMetadataEntry(
					metadata))) {
				typeStore.remove(new MemoryMetadataEntry(
						metadata));
			}
			if (!typeStore.add(new MemoryMetadataEntry(
					metadata))) {
				LOGGER.warn("Unable to add new metadata");
			}

		}
	}

	private class MyMetadataDeleter extends
			MyMetadataReader implements
			MetadataDeleter
	{
		public MyMetadataDeleter(
				final MetadataType type ) {
			super(
					type);
		}

		@Override
		public void close()
				throws Exception {}

		@Override
		public boolean delete(
				final MetadataQuery query ) {
			try (CloseableIterator<GeoWaveMetadata> it = query(query)) {
				while (it.hasNext()) {
					it.next();
					it.remove();
				}
			}
			catch (final IOException e) {
				LOGGER.error(
						"Error deleting by query",
						e);
			}
			return true;
		}

		@Override
		public void flush() {}
	}

	public static class MemoryMetadataEntry implements
			Comparable<MemoryMetadataEntry>
	{
		private final GeoWaveMetadata metadata;

		public MemoryMetadataEntry(
				final GeoWaveMetadata metadata ) {
			this.metadata = metadata;
		}

		public GeoWaveMetadata getMetadata() {
			return metadata;
		}

		@Override
		public int compareTo(
				final MemoryMetadataEntry other ) {
			final Comparator<byte[]> lexyWithNullHandling = Ordering.from(
					UnsignedBytes.lexicographicalComparator()).nullsFirst();
			final int primaryIdCompare = lexyWithNullHandling.compare(
					metadata.getPrimaryId(),
					other.metadata.getPrimaryId());
			if (primaryIdCompare != 0) {
				return primaryIdCompare;
			}
			final int secondaryIdCompare = lexyWithNullHandling.compare(
					metadata.getSecondaryId(),
					other.metadata.getSecondaryId());
			if (secondaryIdCompare != 0) {
				return secondaryIdCompare;
			}
			final int visibilityCompare = lexyWithNullHandling.compare(
					metadata.getVisibility(),
					other.metadata.getVisibility());
			if (visibilityCompare != 0) {
				return visibilityCompare;
			}
			return 0;

		}
	}
}
