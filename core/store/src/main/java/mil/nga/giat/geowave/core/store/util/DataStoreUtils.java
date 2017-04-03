package mil.nga.giat.geowave.core.store.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.IndexMetaData;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.index.SinglePartitionInsertionIds;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import mil.nga.giat.geowave.core.store.adapter.statistics.RowRangeHistogramStatistics;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.visibility.UnconstrainedVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.visibility.UniformVisibilityWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.entities.GeoWaveKey;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.flatten.BitmaskedFieldInfoComparator;
import mil.nga.giat.geowave.core.store.flatten.FlattenedDataSet;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.flatten.FlattenedUnreadDataSingleRow;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;

/*
 */
public class DataStoreUtils
{
	private final static Logger LOGGER = Logger.getLogger(
			DataStoreUtils.class);

	// we append a 0 byte, 8 bytes of timestamp, and 16 bytes of UUID
	public final static int UNIQUE_ADDED_BYTES = 1 + 8 + 16;
	public final static byte UNIQUE_ID_DELIMITER = 0;
	@SuppressWarnings({
		"rawtypes",
		"unchecked"
	})
	public static final UniformVisibilityWriter UNCONSTRAINED_VISIBILITY = new UniformVisibilityWriter(
			new UnconstrainedVisibilityHandler());

	public static final byte[] EMTPY_VISIBILITY = new byte[] {};

	public static List<ByteArrayId> getUniqueDimensionFields(
			final CommonIndexModel model ) {
		final List<ByteArrayId> dimensionFieldIds = new ArrayList<>();
		for (final NumericDimensionField<? extends CommonIndexValue> dimension : model.getDimensions()) {
			if (!dimensionFieldIds.contains(
					dimension.getFieldId())) {
				dimensionFieldIds.add(
						dimension.getFieldId());
			}
		}
		return dimensionFieldIds;
	}

	public static <T> long cardinality(
			final PrimaryIndex index,
			final Map<ByteArrayId, DataStatistics<T>> stats,
			final List<ByteArrayRange> ranges ) {
		final RowRangeHistogramStatistics rangeStats = (RowRangeHistogramStatistics) stats.get(
				RowRangeHistogramStatistics.composeId(
						index.getId()));
		if (rangeStats == null) {
			return Long.MAX_VALUE - 1;
		}
		long count = 0;
		for (final ByteArrayRange range : ranges) {
			count += rangeStats.cardinality(
					range.getStart().getBytes(),
					range.getEnd().getBytes());
		}
		return count;
	}

	public static InsertionIds keysToInsertionIds(
			final GeoWaveKey... geoWaveKeys ) {
		final Map<ByteArrayId, List<ByteArrayId>> sortKeysPerPartition = new HashMap<>();
		for (final GeoWaveKey key : geoWaveKeys) {
			final ByteArrayId partitionKey = new ByteArrayId(
					key.getPartitionKey());
			List<ByteArrayId> sortKeys = sortKeysPerPartition.get(
					partitionKey);
			if (sortKeys == null) {
				sortKeys = new ArrayList<>();
				sortKeysPerPartition.put(
						partitionKey,
						sortKeys);
			}
			sortKeys.add(
					new ByteArrayId(
							key.getSortKey()));
		}
		final Set<SinglePartitionInsertionIds> insertionIds = new HashSet<>();
		for (final Entry<ByteArrayId, List<ByteArrayId>> e : sortKeysPerPartition.entrySet()) {
			insertionIds.add(
					new SinglePartitionInsertionIds(
							e.getKey(),
							e.getValue()));
		}
		return new InsertionIds(
				insertionIds);
	}

	public static boolean rowIdsMatch(
			final GeoWaveKey rowId1,
			final GeoWaveKey rowId2 ) {
		if (!Arrays.equals(
				rowId1.getPartitionKey(),
				rowId2.getPartitionKey())) {
			return false;
		}
		if (!Arrays.equals(
				rowId1.getSortKey(),
				rowId2.getSortKey())) {
			return false;
		}
		if (!Arrays.equals(
				rowId1.getAdapterId(),
				rowId2.getAdapterId())) {
			return false;
		}

		if (Arrays.equals(
				rowId1.getDataId(),
				rowId2.getDataId())) {
			return true;
		}

		return Arrays.equals(
				removeUniqueId(
						rowId1.getDataId()),
				removeUniqueId(
						rowId2.getDataId()));
	}

	public static byte[] removeUniqueId(
			byte[] dataId ) {
		if ((dataId.length < UNIQUE_ADDED_BYTES)
				|| (dataId[dataId.length - UNIQUE_ADDED_BYTES] != UNIQUE_ID_DELIMITER)) {
			return dataId;
		}

		dataId = Arrays.copyOfRange(
				dataId,
				0,
				dataId.length - UNIQUE_ADDED_BYTES);

		return dataId;
	}

	/**
	 *
	 * Takes a byte array representing a serialized composite group of
	 * FieldInfos sharing a common visibility and returns a List of the
	 * individual FieldInfos
	 *
	 * @param compositeFieldId
	 *            the composite bitmask representing the fields contained within
	 *            the flattenedValue
	 * @param flattenedValue
	 *            the serialized composite FieldInfo
	 * @param commonVisibility
	 *            the shared visibility
	 * @param maxFieldPosition
	 *            can short-circuit read and defer decomposition of fields after
	 *            a given position
	 * @return the dataset that has been read
	 */
	public static <T> FlattenedDataSet decomposeFlattenedFields(
			final byte[] bitmask,
			final byte[] flattenedValue,
			final byte[] commonVisibility,
			final int maxFieldPosition ) {
		final List<FlattenedFieldInfo> fieldInfoList = new ArrayList<FlattenedFieldInfo>();
		final List<Integer> fieldPositions = BitmaskUtils.getFieldPositions(
				bitmask);

		final boolean sharedVisibility = fieldPositions.size() > 1;
		if (sharedVisibility) {
			final ByteBuffer input = ByteBuffer.wrap(
					flattenedValue);
			for (int i = 0; i < fieldPositions.size(); i++) {
				final Integer fieldPosition = fieldPositions.get(
						i);
				if ((maxFieldPosition > -1) && (fieldPosition > maxFieldPosition)) {
					return new FlattenedDataSet(
							fieldInfoList,
							new FlattenedUnreadDataSingleRow(
									input,
									i,
									fieldPositions));
				}
				final int fieldLength = input.getInt();
				final byte[] fieldValueBytes = new byte[fieldLength];
				input.get(
						fieldValueBytes);
				fieldInfoList.add(
						new FlattenedFieldInfo(
								fieldPosition,
								fieldValueBytes));
			}
		}
		else {
			fieldInfoList.add(
					new FlattenedFieldInfo(
							fieldPositions.get(
									0),
							flattenedValue));

		}
		return new FlattenedDataSet(
				fieldInfoList,
				null);
	}

	public static QueryRanges constraintsToQueryRanges(
			final List<MultiDimensionalNumericData> constraints,
			final NumericIndexStrategy indexStrategy,
			final int maxRanges,
			final IndexMetaData... hints ) {
		if ((constraints == null) || constraints.isEmpty()) {
			return new QueryRanges(); // implies in negative and
			// positive infinity
		}
		else {
			final List<QueryRanges> ranges = new ArrayList<>(
					constraints.size());
			for (final MultiDimensionalNumericData nd : constraints) {
				ranges.add(
						indexStrategy.getQueryRanges(
								nd,
								maxRanges,
								hints));
			}
			return ranges.size() > 1 ? new QueryRanges(
					ranges)
					: ranges.get(
							0);
		}
	}

	public static String getQualifiedTableName(
			final String tableNamespace,
			final String unqualifiedTableName ) {
		return ((tableNamespace == null) || tableNamespace.isEmpty()) ? unqualifiedTableName
				: tableNamespace + "_" + unqualifiedTableName;
	}

	/**
	 * This method combines all FieldInfos that share a common visibility into a
	 * single FieldInfo
	 *
	 * @param originalList
	 * @return a new list of composite FieldInfos
	 */
	public static <T> List<FieldInfo<?>> composeFlattenedFields(
			final List<FieldInfo<?>> originalList,
			final CommonIndexModel model,
			final WritableDataAdapter<?> writableAdapter ) {
		final List<FieldInfo<?>> retVal = new ArrayList<>();
		final Map<ByteArrayId, List<Pair<Integer, FieldInfo<?>>>> vizToFieldMap = new LinkedHashMap<>();
		boolean sharedVisibility = false;
		// organize FieldInfos by unique visibility
		for (final FieldInfo<?> fieldInfo : originalList) {
			int fieldPosition = writableAdapter.getPositionOfOrderedField(
					model,
					fieldInfo.getDataValue().getId());
			if (fieldPosition == -1) {
				// this is just a fallback for unexpected failures
				fieldPosition = writableAdapter.getPositionOfOrderedField(
						model,
						fieldInfo.getDataValue().getId());
			}
			final ByteArrayId currViz = new ByteArrayId(
					fieldInfo.getVisibility());
			if (vizToFieldMap.containsKey(
					currViz)) {
				sharedVisibility = true;
				final List<Pair<Integer, FieldInfo<?>>> listForViz = vizToFieldMap.get(
						currViz);
				listForViz.add(
						new ImmutablePair<Integer, FieldInfo<?>>(
								fieldPosition,
								fieldInfo));
			}
			else {
				final List<Pair<Integer, FieldInfo<?>>> listForViz = new ArrayList<>();
				listForViz.add(
						new ImmutablePair<Integer, FieldInfo<?>>(
								fieldPosition,
								fieldInfo));
				vizToFieldMap.put(
						currViz,
						listForViz);
			}
		}
		if (!sharedVisibility) {
			// at a minimum, must return transformed (bitmasked) fieldInfos
			final List<FieldInfo<?>> bitmaskedFieldInfos = new ArrayList<>();
			for (final List<Pair<Integer, FieldInfo<?>>> list : vizToFieldMap.values()) {
				// every list must have exactly one element
				final Pair<Integer, FieldInfo<?>> fieldInfo = list.get(
						0);
				bitmaskedFieldInfos.add(
						new FieldInfo<>(
								new PersistentValue<Object>(
										new ByteArrayId(
												BitmaskUtils.generateCompositeBitmask(
														fieldInfo.getLeft())),
										fieldInfo.getRight().getDataValue().getValue()),
								fieldInfo.getRight().getWrittenValue(),
								fieldInfo.getRight().getVisibility()));
			}
			return bitmaskedFieldInfos;
		}
		for (final Entry<ByteArrayId, List<Pair<Integer, FieldInfo<?>>>> entry : vizToFieldMap.entrySet()) {
			final List<byte[]> fieldInfoBytesList = new ArrayList<>();
			int totalLength = 0;
			final SortedSet<Integer> fieldPositions = new TreeSet<Integer>();
			final List<Pair<Integer, FieldInfo<?>>> fieldInfoList = entry.getValue();
			Collections.sort(
					fieldInfoList,
					new BitmaskedFieldInfoComparator());
			for (final Pair<Integer, FieldInfo<?>> fieldInfoPair : fieldInfoList) {
				final FieldInfo<?> fieldInfo = fieldInfoPair.getRight();
				final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(
						4 + fieldInfo.getWrittenValue().length);
				fieldPositions.add(
						fieldInfoPair.getLeft());
				fieldInfoBytes.putInt(
						fieldInfo.getWrittenValue().length);
				fieldInfoBytes.put(
						fieldInfo.getWrittenValue());
				fieldInfoBytesList.add(
						fieldInfoBytes.array());
				totalLength += fieldInfoBytes.array().length;
			}
			final ByteBuffer allFields = ByteBuffer.allocate(
					totalLength);
			for (final byte[] bytes : fieldInfoBytesList) {
				allFields.put(
						bytes);
			}
			final byte[] compositeBitmask = BitmaskUtils.generateCompositeBitmask(
					fieldPositions);
			final FieldInfo<?> composite = new FieldInfo<T>(
					new PersistentValue<T>(
							new ByteArrayId(
									compositeBitmask),
							null), // unnecessary
					allFields.array(),
					entry.getKey().getBytes());
			retVal.add(
					composite);
		}
		return retVal;
	}

	public static ByteArrayId ensureUniqueId(
			final byte[] id,
			final boolean hasMetadata ) {

		final ByteBuffer buf = ByteBuffer.allocate(
				id.length + UNIQUE_ADDED_BYTES);

		byte[] metadata = null;
		byte[] dataId;
		if (hasMetadata) {
			final int metadataStartIdx = id.length - 12;
			final byte[] lengths = Arrays.copyOfRange(
					id,
					metadataStartIdx,
					id.length);

			final ByteBuffer lengthsBuf = ByteBuffer.wrap(
					lengths);
			final int adapterIdLength = lengthsBuf.getInt();
			int dataIdLength = lengthsBuf.getInt();
			dataIdLength += UNIQUE_ADDED_BYTES;
			final int duplicates = lengthsBuf.getInt();

			final ByteBuffer newLengths = ByteBuffer.allocate(
					12);
			newLengths.putInt(
					adapterIdLength);
			newLengths.putInt(
					dataIdLength);
			newLengths.putInt(
					duplicates);
			newLengths.rewind();
			metadata = newLengths.array();
			dataId = Arrays.copyOfRange(
					id,
					0,
					metadataStartIdx);
		}
		else {
			dataId = id;
		}

		buf.put(
				dataId);

		final long timestamp = System.currentTimeMillis();
		buf.put(
				new byte[] {
					UNIQUE_ID_DELIMITER
				});
		final UUID uuid = UUID.randomUUID();
		buf.putLong(
				timestamp);
		buf.putLong(
				uuid.getLeastSignificantBits());
		buf.putLong(
				uuid.getMostSignificantBits());
		if (hasMetadata) {
			buf.put(
					metadata);
		}

		return new ByteArrayId(
				buf.array());
	}

	private static final byte[] BEG_AND_BYTE = "&".getBytes(
			StringUtils.GEOWAVE_CHAR_SET);
	private static final byte[] END_AND_BYTE = ")".getBytes(
			StringUtils.GEOWAVE_CHAR_SET);

	public static byte[] mergeVisibilities(
			final byte vis1[],
			final byte vis2[] ) {
		if ((vis1 == null) || (vis1.length == 0)) {
			return vis2;
		}
		else if ((vis2 == null) || (vis2.length == 0)) {
			return vis1;
		}

		final ByteBuffer buffer = ByteBuffer.allocate(
				vis1.length + 3 + vis2.length);
		buffer.putChar(
				'(');
		buffer.put(
				vis1);
		buffer.putChar(
				')');
		buffer.put(
				BEG_AND_BYTE);
		buffer.put(
				vis2);
		buffer.put(
				END_AND_BYTE);
		return buffer.array();
	}
}
