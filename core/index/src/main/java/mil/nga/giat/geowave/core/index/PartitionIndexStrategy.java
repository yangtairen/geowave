package mil.nga.giat.geowave.core.index;

import java.util.List;

import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public interface PartitionIndexStrategy extends
		Persistable
{
	public ByteArrayId getInsertionPartitionKey(
			MultiDimensionalNumericData insertionData );

	public List<ByteArrayId> getQueryPartitionKeys(
			MultiDimensionalNumericData queryData );
	/***
	 * Get the offset in bytes before the dimensional index. This can accounts
	 * for tier IDs and bin IDs
	 * 
	 * @return the byte offset prior to the dimensional index
	 */
	public int getPartitionKeyLength();
}
