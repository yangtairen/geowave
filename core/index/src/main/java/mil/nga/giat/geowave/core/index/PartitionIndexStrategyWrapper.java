package mil.nga.giat.geowave.core.index;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

public class PartitionIndexStrategyWrapper implements NumericIndexStrategy
{
	private PartitionIndexStrategy partitionIndexStrategy;

	public PartitionIndexStrategyWrapper(
			PartitionIndexStrategy partitionIndexStrategy ) {
		this.partitionIndexStrategy = partitionIndexStrategy;
	}

	@Override
	public QueryRanges getQueryRanges(
			MultiDimensionalNumericData indexedRange,
			IndexMetaData... hints ) {
		return null;
	}

	@Override
	public QueryRanges getQueryRanges(
			MultiDimensionalNumericData indexedRange,
			int maxEstimatedRangeDecomposition,
			IndexMetaData... hints ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InsertionIds getInsertionIds(
			MultiDimensionalNumericData indexedData ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public InsertionIds getInsertionIds(
			MultiDimensionalNumericData indexedData,
			int maxEstimatedDuplicateIds ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			InsertionIds insertionId ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<ByteArrayId> getPartitionKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		return Collections.emptyList();
	}

	@Override
	public byte[] toBinary() {
		return PersistenceUtils.toBinary(
				partitionIndexStrategy);
	}

	@Override
	public void fromBinary(
			byte[] bytes ) {
		partitionIndexStrategy = PersistenceUtils.fromBinary(
				bytes,
				PartitionIndexStrategy.class);
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			ByteArrayId sortKey ) {
		return new MultiDimensionalCoordinates();
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			MultiDimensionalNumericData dataRange,
			IndexMetaData... hints ) {
		return null;
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return null;
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return null;
	}

	@Override
	public int getPartitionKeyLength() {
		return partitionIndexStrategy.getPartitionKeyLength();
	}
}
