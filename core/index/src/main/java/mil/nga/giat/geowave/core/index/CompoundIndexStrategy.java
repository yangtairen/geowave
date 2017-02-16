package mil.nga.giat.geowave.core.index;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.dimension.NumericDimensionDefinition;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

/**
 * Class that implements a compound index strategy. It's a wrapper around two
 * NumericIndexStrategy objects that can externally be treated as a
 * multi-dimensional NumericIndexStrategy.
 *
 * Each of the 'wrapped' strategies cannot share the same dimension definition.
 *
 */
public class CompoundIndexStrategy implements
		NumericIndexStrategy
{

	private PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> subStrategy1;
	private NumericIndexStrategy subStrategy2;
	private NumericDimensionDefinition[] baseDefinitions;
	private double[] highestPrecision;
	private int defaultNumberOfRanges;
	private int metaDataSplit = -1;

	public CompoundIndexStrategy(
			final PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> subStrategy1,
			final NumericIndexStrategy subStrategy2 ) {
		this.subStrategy1 = subStrategy1;
		this.subStrategy2 = subStrategy2;
		defaultNumberOfRanges = (int) Math.ceil(
				Math.pow(
						2,
						getNumberOfDimensions()));
	}

	protected CompoundIndexStrategy() {}

	public PartitionIndexStrategy<MultiDimensionalNumericData, MultiDimensionalNumericData> getPrimarySubStrategy() {
		return subStrategy1;
	}

	public NumericIndexStrategy getSecondarySubStrategy() {
		return subStrategy2;
	}

	@Override
	public byte[] toBinary() {
		final byte[] delegateBinary1 = PersistenceUtils.toBinary(
				subStrategy1);
		final byte[] delegateBinary2 = PersistenceUtils.toBinary(
				subStrategy2);
		final ByteBuffer buf = ByteBuffer.allocate(
				4 + delegateBinary1.length + delegateBinary2.length);
		buf.putInt(
				delegateBinary1.length);
		buf.put(
				delegateBinary1);
		buf.put(
				delegateBinary2);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(
				bytes);
		final int delegateBinary1Length = buf.getInt();
		final byte[] delegateBinary1 = new byte[delegateBinary1Length];
		buf.get(
				delegateBinary1);
		final byte[] delegateBinary2 = new byte[bytes.length - delegateBinary1Length - 4];
		buf.get(
				delegateBinary2);
		subStrategy1 = PersistenceUtils.fromBinary(
				delegateBinary1,
				PartitionIndexStrategy.class);
		subStrategy2 = PersistenceUtils.fromBinary(
				delegateBinary2,
				NumericIndexStrategy.class);
		defaultNumberOfRanges = (int) Math.ceil(
				Math.pow(
						2,
						getNumberOfDimensions()));
	}

	/**
	 * Get the total number of dimensions from all sub-strategies
	 *
	 * @return the number of dimensions
	 */
	public int getNumberOfDimensions() {
		return baseDefinitions.length;
	}

	/**
	 * Create a compound ByteArrayId
	 *
	 * @param id1
	 *            ByteArrayId for the first sub-strategy
	 * @param id2
	 *            ByteArrayId for the second sub-strategy
	 * @return the ByteArrayId for the compound strategy
	 */
	public ByteArrayId composeByteArrayId(
			final ByteArrayId id1,
			final ByteArrayId id2 ) {
		final byte[] bytes = new byte[id1.getBytes().length + id2.getBytes().length + 4];
		final ByteBuffer buf = ByteBuffer.wrap(
				bytes);
		buf.put(
				id1.getBytes());
		buf.put(
				id2.getBytes());
		buf.putInt(
				id1.getBytes().length);
		return new ByteArrayId(
				bytes);
	}

	/**
	 * Get the ByteArrayId for each sub-strategy from the ByteArrayId for the
	 * compound index strategy
	 *
	 * @param id
	 *            the compound ByteArrayId
	 * @return the ByteArrayId for each sub-strategy
	 */
	public ByteArrayId[] decomposeByteArrayId(
			final ByteArrayId id ) {
		final ByteBuffer buf = ByteBuffer.wrap(
				id.getBytes());
		final int id1Length = buf.getInt(
				id.getBytes().length - 4);
		final byte[] bytes1 = new byte[id1Length];
		final byte[] bytes2 = new byte[id.getBytes().length - id1Length - 4];
		buf.get(
				bytes1);
		buf.get(
				bytes2);
		return new ByteArrayId[] {
			new ByteArrayId(
					bytes1),
			new ByteArrayId(
					bytes2)
		};
	}
	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final IndexMetaData... hints ) {
		return getQueryRanges(
				indexedRange,
				-1,
				hints);
	}

	@Override
	public QueryRanges getQueryRanges(
			final MultiDimensionalNumericData indexedRange,
			final int maxEstimatedRangeDecomposition,
			final IndexMetaData... hints ) {
		final Set<ByteArrayId> partitionIds = subStrategy1.getQueryPartitionKeys(
				indexedRange,
				extractHints(
						hints,
						0));
		final QueryRanges queryRanges = subStrategy2.getQueryRanges(
				indexedRange,
				extractHints(
						hints,
						1));

		return new QueryRanges(
				partitionIds,
				queryRanges);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData ) {
		return getInsertionIds(
				indexedData,
				defaultNumberOfRanges);
	}

	@Override
	public InsertionIds getInsertionIds(
			final MultiDimensionalNumericData indexedData,
			final int maxEstimatedDuplicateIds ) {
		final ByteArrayId partitionKey = subStrategy1.getInsertionPartitionKey(
				indexedData);
		final InsertionIds insertionIds2 = subStrategy2.getInsertionIds(
				indexedData,
				maxEstimatedDuplicateIds);
		return new InsertionIds(
				partitionKey,
				insertionIds2);
	}

	@Override
	public MultiDimensionalNumericData getRangeForId(
			final InsertionIds insertionId ) {
		return subStrategy2.getRangeForId(
				insertionId);
	}

	@Override
	public MultiDimensionalCoordinates getCoordinatesPerDimension(
			final ByteArrayId insertionId ) {
		final ByteArrayId[] insertionIds = decomposeByteArrayId(
				insertionId);
		return subStrategy2.getCoordinatesPerDimension(
				insertionIds[1]);
	}

	@Override
	public NumericDimensionDefinition[] getOrderedDimensionDefinitions() {
		return baseDefinitions;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = (prime * result) + Arrays.hashCode(
				baseDefinitions);
		result = (prime * result) + defaultNumberOfRanges;
		result = (prime * result) + ((subStrategy1 == null) ? 0 : subStrategy1.hashCode());
		result = (prime * result) + ((subStrategy2 == null) ? 0 : subStrategy2.hashCode());
		return result;
	}

	@Override
	public boolean equals(
			final Object obj ) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		final CompoundIndexStrategy other = (CompoundIndexStrategy) obj;
		if (!Arrays.equals(
				baseDefinitions,
				other.baseDefinitions)) {
			return false;
		}
		if (defaultNumberOfRanges != other.defaultNumberOfRanges) {
			return false;
		}
		if (subStrategy1 == null) {
			if (other.subStrategy1 != null) {
				return false;
			}
		}
		else if (!subStrategy1.equals(
				other.subStrategy1)) {
			return false;
		}
		if (subStrategy2 == null) {
			if (other.subStrategy2 != null) {
				return false;
			}
		}
		else if (!subStrategy2.equals(
				other.subStrategy2)) {
			return false;
		}
		return true;
	}

	@Override
	public String getId() {
		return StringUtils.intToString(
				hashCode());
	}

	@Override
	public double[] getHighestPrecisionIdRangePerDimension() {
		return highestPrecision;
	}

	@Override
	public Set<ByteArrayId> getPartitionKeys() {
		final Set<ByteArrayId> partitionKeys1 = subStrategy1.getPartitionKeys();
		final Set<ByteArrayId> partitionKeys2 = subStrategy2.getPartitionKeys();
		Set<ByteArrayId> partitionKeys;
		if ((partitionKeys1 == null) || partitionKeys1.isEmpty()) {
			partitionKeys = partitionKeys2;
		}
		else if ((partitionKeys2 == null) || partitionKeys2.isEmpty()) {
			partitionKeys = partitionKeys1;
		}
		else {
			// use all permutations of range keys
			partitionKeys = new HashSet<ByteArrayId>(
					partitionKeys1.size() * partitionKeys2.size());
			for (final ByteArrayId partitionKey1 : partitionKeys1) {
				for (final ByteArrayId partitionKey2 : partitionKeys2) {
					partitionKeys.add(
							new ByteArrayId(
									ByteArrayUtils.combineArrays(
											partitionKey1.getBytes(),
											partitionKey2.getBytes())));
				}
			}
		}
		return partitionKeys;
	}

	@Override
	public int getPartitionKeyLength() {
		return subStrategy1.getPartitionKeyLength() + subStrategy2.getPartitionKeyLength();
	}

	@Override
	public List<IndexMetaData> createMetaData() {
		final List<IndexMetaData> result = new ArrayList<IndexMetaData>();
		for (final IndexMetaData metaData : subStrategy1.createMetaData()) {
			result.add(
					new CompoundIndexMetaDataWrapper(
							metaData,
							0));
		}
		metaDataSplit = result.size();
		for (final IndexMetaData metaData : subStrategy2.createMetaData()) {
			result.add(
					new CompoundIndexMetaDataWrapper(
							metaData,
							1));
		}
		return result;
	}

	private int getMetaDataSplit() {
		if (metaDataSplit == -1) {
			metaDataSplit = subStrategy1.createMetaData().size();
		}
		return metaDataSplit;
	}

	private IndexMetaData[] extractHints(
			final IndexMetaData[] hints,
			final int indexNo ) {
		if ((hints == null) || (hints.length == 0)) {
			return hints;
		}
		final int splitPoint = getMetaDataSplit();
		final int start = (indexNo == 0) ? 0 : splitPoint;
		final int stop = (indexNo == 0) ? splitPoint : hints.length;
		final IndexMetaData[] result = new IndexMetaData[stop - start];
		int p = 0;
		for (int i = start; i < stop; i++) {
			result[p++] = ((CompoundIndexMetaDataWrapper) hints[i]).metaData;
		}
		return result;
	}

	/**
	 * Get the ByteArrayId for each sub-strategy from the ByteArrayId for the
	 * compound index strategy
	 *
	 * @param id
	 *            the compound ByteArrayId
	 * @return the ByteArrayId for each sub-strategy
	 */
	public static ByteArrayId extractByteArrayId(
			final ByteArrayId id,
			final int index ) {
		final ByteBuffer buf = ByteBuffer.wrap(
				id.getBytes());
		final int id1Length = buf.getInt(
				id.getBytes().length - 4);

		if (index == 0) {
			final byte[] bytes1 = new byte[id1Length];
			buf.get(
					bytes1);
			return new ByteArrayId(
					bytes1);
		}

		final byte[] bytes2 = new byte[id.getBytes().length - id1Length - 4];
		buf.position(
				id1Length);
		buf.get(
				bytes2);
		return new ByteArrayId(
				bytes2);

	}

	/**
	 *
	 * Delegate Metadata item for an underlying index. For
	 * CompoundIndexStrategy, this delegate wraps the meta data for one of the
	 * two indices. The primary function of this class is to extract out the
	 * parts of the ByteArrayId that are specific to each index during an
	 * 'update' operation.
	 *
	 */
	private static class CompoundIndexMetaDataWrapper implements
			IndexMetaData,
			Persistable
	{

		private IndexMetaData metaData;
		private int index;

		public CompoundIndexMetaDataWrapper() {}

		public CompoundIndexMetaDataWrapper(
				final IndexMetaData metaData,
				final int index ) {
			super();
			this.metaData = metaData;
			this.index = index;
		}

		@Override
		public byte[] toBinary() {
			final byte[] metaBytes = PersistenceUtils.toBinary(
					metaData);
			final ByteBuffer buf = ByteBuffer.allocate(
					4 + metaBytes.length);
			buf.put(
					metaBytes);
			buf.putInt(
					index);
			return buf.array();
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			final ByteBuffer buf = ByteBuffer.wrap(
					bytes);
			final byte[] metaBytes = new byte[bytes.length - 4];
			buf.get(
					metaBytes);
			metaData = PersistenceUtils.fromBinary(
					metaBytes,
					IndexMetaData.class);
			index = buf.getInt();
		}

		@Override
		public void merge(
				final Mergeable merge ) {
			if (merge instanceof CompoundIndexMetaDataWrapper) {
				final CompoundIndexMetaDataWrapper compound = (CompoundIndexMetaDataWrapper) merge;
				metaData.merge(
						compound.metaData);
			}
		}

		@Override
		public void insertionIdsAdded(
				final List<ByteArrayId> ids ) {
			metaData.insertionIdsAdded(
					Lists.transform(
							ids,
							new Function<ByteArrayId, ByteArrayId>() {
								@Override
								public ByteArrayId apply(
										final ByteArrayId input ) {
									return extractByteArrayId(
											input,
											index);
								}
							}));
		}

		@Override
		public void insertionIdsRemoved(
				final List<ByteArrayId> ids ) {
			metaData.insertionIdsRemoved(
					Lists.transform(
							ids,
							new Function<ByteArrayId, ByteArrayId>() {
								@Override
								public ByteArrayId apply(
										final ByteArrayId input ) {
									return extractByteArrayId(
											input,
											index);
								}
							}));
		}
	}

	@Override
	public MultiDimensionalCoordinateRanges[] getCoordinateRangesPerDimension(
			final MultiDimensionalNumericData dataRange,
			final IndexMetaData... hints ) {
		return subStrategy2.getCoordinateRangesPerDimension(
				dataRange,
				hints);
	}

	@Override
	public ByteArrayId getInsertionPartitionKey(
			final MultiDimensionalNumericData insertionData ) {
		final ByteArrayId partitionKey1 = subStrategy1.getInsertionPartitionKey(
				insertionData);
		final ByteArrayId partitionKey2 = subStrategy2.getInsertionPartitionKey(
				insertionData);
		if (partitionKey1 == null) {
			return partitionKey2;
		}
		if (partitionKey2 == null) {
			return partitionKey1;
		}
		return new ByteArrayId(
				ByteArrayUtils.combineArrays(
						partitionKey1.getBytes(),
						partitionKey2.getBytes()));
	}

	@Override
	public Set<ByteArrayId> getQueryPartitionKeys(
			final MultiDimensionalNumericData queryData,
			final IndexMetaData... hints ) {
		final Set<ByteArrayId> partitionKeys1 = subStrategy1.getQueryPartitionKeys(
				queryData,hints);
		final Set<ByteArrayId> partitionKeys2 = subStrategy2.getQueryPartitionKeys(
				queryData,hints);
		if (partitionKeys1 == null || partitionKeys1.isEmpty()) {
			return partitionKeys2;
		}
		if (partitionKeys2 == null || partitionKeys2.isEmpty()) {
			return partitionKeys1;
		}
		//return all permutations of partitionKeys
		Set<ByteArrayId> partitionKeys = new HashSet<ByteArrayId>(
				partitionKeys1.size() * partitionKeys2.size());
		for (final ByteArrayId partitionKey1 : partitionKeys1) {
			for (final ByteArrayId partitionKey2 : partitionKeys2) {
				partitionKeys.add(
						new ByteArrayId(
								ByteArrayUtils.combineArrays(
										partitionKey1.getBytes(),
										partitionKey2.getBytes())));
			}
		}
		return partitionKeys;
	}
}
