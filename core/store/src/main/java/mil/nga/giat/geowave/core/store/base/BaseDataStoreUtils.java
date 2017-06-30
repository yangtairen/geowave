package mil.nga.giat.geowave.core.store.base;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.IntermediaryWriteEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.data.DataWriter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.flatten.BitmaskUtils;
import mil.nga.giat.geowave.core.store.flatten.BitmaskedPairComparator;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class BaseDataStoreUtils
{
	private final static Logger LOGGER = Logger.getLogger(BaseDataStoreUtils.class);
	public static final int MAX_RANGE_DECOMPOSITION = 2000;
	public static final int AGGREGATION_RANGE_DECOMPOSITION = 10;

	public static <T> GeoWaveRow[] getGeoWaveRows(
			final T entry,
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		return getWriteInfo(
				entry,
				adapter,
				index,
				customFieldVisibilityWriter).getRows();
	}

	protected static <T> IntermediaryWriteEntryInfo getWriteInfo(
			final T entry,
			final WritableDataAdapter<T> adapter,
			final PrimaryIndex index,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final CommonIndexModel indexModel = index.getIndexModel();

		final AdapterPersistenceEncoding encodedData = adapter.encode(
				entry,
				indexModel);
		final InsertionIds insertionIds = encodedData.getInsertionIds(index);
		final PersistentDataset extendedData = encodedData.getAdapterExtendedData();
		final PersistentDataset indexedData = encodedData.getCommonData();
		final List<PersistentValue> extendedValues = extendedData.getValues();
		final List<PersistentValue> commonValues = indexedData.getValues();

		List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>();

		final byte[] dataId = adapter.getDataId(
				entry).getBytes();
		final byte[] adapterId = adapter.getAdapterId().getBytes();
		if (!insertionIds.isEmpty()) {
			for (final PersistentValue fieldValue : commonValues) {
				final FieldInfo<?> fieldInfo = getFieldInfo(
						indexModel,
						fieldValue,
						entry,
						customFieldVisibilityWriter);
				if (fieldInfo != null) {
					fieldInfoList.add(fieldInfo);
				}
			}
			for (final PersistentValue<?> fieldValue : extendedValues) {
				if (fieldValue.getValue() != null) {
					final FieldInfo<?> fieldInfo = getFieldInfo(
							adapter,
							fieldValue,
							entry,
							customFieldVisibilityWriter);
					if (fieldInfo != null) {
						fieldInfoList.add(fieldInfo);
					}
				}
			}
		}
		else {
			LOGGER.warn("Indexing failed to produce insertion ids; entry [" + adapter.getDataId(
					entry).getString() + "] not saved.");
		}

		fieldInfoList = BaseDataStoreUtils.composeFlattenedFields(
				fieldInfoList,
				index.getIndexModel(),
				adapter);
		// TODO GEOWAVE-1018 need to figure out the correct way to do this for
		// all data stores
		byte[] uniqueDataId;
		// if ((adapter instanceof RowMergingDataAdapter) &&
		// (((RowMergingDataAdapter) adapter).getTransform() != null)) {
		// uniqueDataId = DataStoreUtils.ensureUniqueId(
		// dataId,
		// false).getBytes();
		// }
		// else {
		uniqueDataId = dataId;
		// }

		return new IntermediaryWriteEntryInfo(
				uniqueDataId,
				adapterId,
				insertionIds,
				fieldInfoList);
	}

	/**
	 * This method combines all FieldInfos that share a common visibility into a
	 * single FieldInfo
	 *
	 * @param originalList
	 * @return a new list of composite FieldInfos
	 */
	private static <T> List<FieldInfo<?>> composeFlattenedFields(
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
			if (vizToFieldMap.containsKey(currViz)) {
				sharedVisibility = true;
				final List<Pair<Integer, FieldInfo<?>>> listForViz = vizToFieldMap.get(currViz);
				listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(
						fieldPosition,
						fieldInfo));
			}
			else {
				final List<Pair<Integer, FieldInfo<?>>> listForViz = new ArrayList<>();
				listForViz.add(new ImmutablePair<Integer, FieldInfo<?>>(
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
				final Pair<Integer, FieldInfo<?>> fieldInfo = list.get(0);
				bitmaskedFieldInfos.add(new FieldInfo<>(
						new PersistentValue<Object>(
								new ByteArrayId(
										BitmaskUtils.generateCompositeBitmask(fieldInfo.getLeft())),
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
					new BitmaskedPairComparator());
			for (final Pair<Integer, FieldInfo<?>> fieldInfoPair : fieldInfoList) {
				final FieldInfo<?> fieldInfo = fieldInfoPair.getRight();
				final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(4 + fieldInfo.getWrittenValue().length);
				fieldPositions.add(fieldInfoPair.getLeft());
				fieldInfoBytes.putInt(fieldInfo.getWrittenValue().length);
				fieldInfoBytes.put(fieldInfo.getWrittenValue());
				fieldInfoBytesList.add(fieldInfoBytes.array());
				totalLength += fieldInfoBytes.array().length;
			}
			final ByteBuffer allFields = ByteBuffer.allocate(totalLength);
			for (final byte[] bytes : fieldInfoBytesList) {
				allFields.put(bytes);
			}
			final byte[] compositeBitmask = BitmaskUtils.generateCompositeBitmask(fieldPositions);
			final FieldInfo<?> composite = new FieldInfo<T>(
					new PersistentValue<T>(
							new ByteArrayId(
									compositeBitmask),
							null), // unnecessary
					allFields.array(),
					entry.getKey().getBytes());
			retVal.add(composite);
		}
		return retVal;
	}

	private static <T> FieldInfo<?> getFieldInfo(
			final DataWriter dataWriter,
			final PersistentValue<?> fieldValue,
			final T entry,
			final VisibilityWriter<T> customFieldVisibilityWriter ) {
		final FieldWriter fieldWriter = dataWriter.getWriter(fieldValue.getId());
		final FieldVisibilityHandler<T, Object> customVisibilityHandler = customFieldVisibilityWriter
				.getFieldVisibilityHandler(fieldValue.getId());
		if (fieldWriter != null) {
			final Object value = fieldValue.getValue();
			return new FieldInfo(
					fieldValue,
					fieldWriter.writeField(value),
					DataStoreUtils.mergeVisibilities(
							customVisibilityHandler.getVisibility(
									entry,
									fieldValue.getId(),
									value),
							fieldWriter.getVisibility(
									entry,
									fieldValue.getId(),
									value)));
		}
		else if (fieldValue.getValue() != null) {
			LOGGER.warn("Data writer of class " + dataWriter.getClass() + " does not support field for "
					+ fieldValue.getValue());
		}
		return null;
	}
}
