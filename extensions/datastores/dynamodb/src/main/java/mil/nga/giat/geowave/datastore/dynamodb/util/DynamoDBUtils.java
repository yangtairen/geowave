package mil.nga.giat.geowave.datastore.dynamodb.util;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.entities.GeowaveRowId;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.flatten.FlattenedFieldInfo;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBIndexWriter;

public class DynamoDBUtils
{
	private final static Logger LOGGER = LoggerFactory.getLogger(
			DynamoDBUtils.class);

	@SuppressWarnings("unchecked")
	public static <T> T decodeRow(
			final Map<String, AttributeValue> row,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T> scanCallback ) {
		final ByteBuffer idBuf = row.get(
				DynamoDBIndexWriter.GW_ID_KEY).getB();
		final int totalLength = idBuf.array().length;
		final byte[] dataId = new byte[idBuf.getInt()];
		idBuf.get(
				dataId);

		final byte[] adapterId = new byte[totalLength - dataId.length - 4];
		idBuf.get(
				adapterId);
		final GeowaveRowId rowId = new GeowaveRowId(
				row.get(
						DynamoDBIndexWriter.GW_IDX_KEY).getB().array(),
				dataId,
				adapterId,
				1);
		return (T) decodeRowObj(
				row,
				rowId,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
	}

	private static <T> Object decodeRowObj(
			final Map<String, AttributeValue> row,
			final GeowaveRowId rowId,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T> scanCallback ) {
		final Pair<T, DataStoreEntryInfo> pair = decodeRow(
				row,
				rowId,
				null,
				adapterStore,
				clientFilter,
				index,
				scanCallback);
		return pair != null ? pair.getLeft() : null;

	}

	@SuppressWarnings("unchecked")
	public static <T> Pair<T, DataStoreEntryInfo> decodeRow(
			final Map<String, AttributeValue> row,
			final GeowaveRowId rowId,
			DataAdapter<T> dataAdapter,
			final AdapterStore adapterStore,
			final QueryFilter clientFilter,
			final PrimaryIndex index,
			final ScanCallback<T> scanCallback ) {
		if (dataAdapter == null) {
			if (adapterStore != null) {
				dataAdapter = (DataAdapter<T>) adapterStore.getAdapter(
						new ByteArrayId(
								rowId.getAdapterId()));
			}
			if (dataAdapter == null) {
				LOGGER.error(
						"Could not decode row from iterator. Either adapter or adapter store must be non-null.");
				return null;
			}
		}

		// build a persistence encoding object first, pass it through the
		// client filters and if its accepted, use the data adapter to
		// decode the persistence model into the native data type
		final PersistentDataset<CommonIndexValue> indexData = new PersistentDataset<CommonIndexValue>();
		final PersistentDataset<Object> extendedData = new PersistentDataset<Object>();
		final PersistentDataset<byte[]> unknownData = new PersistentDataset<byte[]>();

		final List<FieldInfo<?>> fieldInfoList = new ArrayList<FieldInfo<?>>();

		final List<FlattenedFieldInfo> flattenedFieldInfoList = new ArrayList<FlattenedFieldInfo>();
		final CommonIndexModel indexModel = index.getIndexModel();
		final byte[] flattenedValue = row.get(
				DynamoDBIndexWriter.GW_VALUE_KEY).getB().array();
		final ByteBuffer input = ByteBuffer.wrap(
				flattenedValue);
		int i = 0;
		while (input.hasRemaining()) {
			final int fieldLength = input.getInt();
			final byte[] fieldValueBytes = new byte[fieldLength];
			input.get(
					fieldValueBytes);
			flattenedFieldInfoList.add(
					new FlattenedFieldInfo(
							i++,
							fieldValueBytes));
		}
		for (final FlattenedFieldInfo fieldInfo : flattenedFieldInfoList) {
			final ByteArrayId fieldId = dataAdapter.getFieldIdForPosition(
					indexModel,
					fieldInfo.getFieldPosition());
			final FieldReader<? extends CommonIndexValue> indexFieldReader = indexModel.getReader(
					fieldId);
			if (indexFieldReader != null) {
				final CommonIndexValue indexValue = indexFieldReader.readField(
						fieldInfo.getValue());
				final PersistentValue<CommonIndexValue> val = new PersistentValue<CommonIndexValue>(
						fieldId,
						indexValue);
				indexData.addValue(
						val);
				fieldInfoList.add(
						DataStoreUtils.getFieldInfo(
								val,
								fieldInfo.getValue(),
								new byte[] {}));
			}
			else {
				final FieldReader<?> extFieldReader = dataAdapter.getReader(
						fieldId);
				if (extFieldReader != null) {
					final Object value = extFieldReader.readField(
							fieldInfo.getValue());
					final PersistentValue<Object> val = new PersistentValue<Object>(
							fieldId,
							value);
					extendedData.addValue(
							val);
					fieldInfoList.add(
							DataStoreUtils.getFieldInfo(
									val,
									fieldInfo.getValue(),
									new byte[] {}));
				}
				else {
					LOGGER.error(
							"field reader not found for data entry, the value may be ignored");
					unknownData.addValue(
							new PersistentValue<byte[]>(
									fieldId,
									fieldInfo.getValue()));
				}
			}
		}
		final IndexedAdapterPersistenceEncoding encodedRow = new IndexedAdapterPersistenceEncoding(
				dataAdapter.getAdapterId(),
				new ByteArrayId(
						rowId.getDataId()),
				new ByteArrayId(
						rowId.getInsertionId()),
				rowId.getNumberOfDuplicates(),
				indexData,
				unknownData,
				extendedData);
		if ((clientFilter == null) || clientFilter.accept(
				index.getIndexModel(),
				encodedRow)) {
			final Pair<T, DataStoreEntryInfo> pair = Pair.of(
					dataAdapter.decode(
							encodedRow,
							index),
					new DataStoreEntryInfo(
							rowId.getDataId(),
							Arrays.asList(
									new ByteArrayId(
											rowId.getInsertionId())),
							Arrays.asList(
									new ByteArrayId(
											rowId.getInsertionId())),
							fieldInfoList));
			if (scanCallback != null) {
				scanCallback.entryScanned(
						pair.getRight(),
						pair.getLeft());
			}
			return pair;
		}
		return null;
	}
}
