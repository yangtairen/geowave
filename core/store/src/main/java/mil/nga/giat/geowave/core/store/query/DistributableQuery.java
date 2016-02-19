package mil.nga.giat.geowave.core.store.query;

import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.SecondaryIndex;

/**
 * This interface fully describes a query and is persistable so that it can be
 * distributed if necessary (particularly useful for using a query as mapreduce
 * input)
 */
public interface DistributableQuery extends
		Query,
		Persistable
{
	/**
	 * Return a collection of ranges over secondary index keys
	 * 
	 * @param index
	 *            the index to extract constraints for
	 * @return A collection of ranges over secondary index keys.
	 */
	public List<ByteArrayRange> getSecondaryIndexRanges(
			SecondaryIndex<?> index );

	public List<DistributableQueryFilter> getSecondaryQueryFilter(
			SecondaryIndex<?> index );
}
