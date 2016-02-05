package mil.nga.giat.geowave.core.store.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

/**
 * This is responsible for persisting secondary index entries
 */
public interface SecondaryIndexDataStore
{
	/**
	 * Writes index attributes to a secondary index
	 * 
	 * @param secondaryIndex
	 * @param primaryIndexId
	 * @param primaryIndexRowId
	 * @param indexedAttributes
	 */
	public void store(
			SecondaryIndex<?> secondaryIndex,
			ByteArrayId primaryIndexId,
			ByteArrayId primaryIndexRowId,
			List<FieldInfo<?>> indexedAttributes );

	/**
	 * Deletes indexed attributes from a secondary index
	 * 
	 * @param secondaryIndex
	 * @param primaryIndexId
	 * @param indexedAttributes
	 */
	public void delete(
			final SecondaryIndex<?> secondaryIndex,
			final List<FieldInfo<?>> indexedAttributes );

	/**
	 * 
	 * Returns matching primary index row IDs for a secondary index lookup
	 * 
	 * @param secondaryIndex
	 * @param ranges
	 * @param constraints
	 * @param primaryIndexId
	 * @param visibility
	 * @return
	 */
	public CloseableIterator<ByteArrayId> query(
			SecondaryIndex<?> secondaryIndex,
			List<ByteArrayRange> ranges,
			List<DistributableQueryFilter> constraints,
			ByteArrayId primaryIndexId,
			String... visibility );

	public void flush();
}
