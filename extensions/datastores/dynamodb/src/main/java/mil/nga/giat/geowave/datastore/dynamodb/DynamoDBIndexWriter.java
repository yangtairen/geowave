package mil.nga.giat.geowave.datastore.dynamodb;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.dynamodbv2.util.TableUtils;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo;
import mil.nga.giat.geowave.core.store.base.DataStoreEntryInfo.FieldInfo;
import mil.nga.giat.geowave.core.store.callback.IngestCallback;
import mil.nga.giat.geowave.core.store.data.VisibilityWriter;
import mil.nga.giat.geowave.core.store.index.DataStoreIndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

public class DynamoDBIndexWriter<T> extends
		DataStoreIndexWriter<T, WriteRequest>
{
	public static final String GW_ID_KEY = "I";
	public static final String GW_IDX_KEY = "X";
	public static final String GW_VALUE_KEY = "V";
	protected final AmazonDynamoDBAsyncClient client;
	protected final DynamoDBOperations dynamodbOperations;
	private static Map<String, Boolean> tableExistsCache = new HashMap<>();

	public DynamoDBIndexWriter(
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final DynamoDBOperations operations,
			final IngestCallback<T> callback,
			final Closeable closable ) {
		super(
				adapter,
				index,
				null,
				null,
				callback,
				closable);
		this.client = operations.getClient();
		this.dynamodbOperations = operations;
	}

	private synchronized void ensureOpen() {
		if (writer == null) {
			final String tableName = dynamodbOperations.getQualifiedTableName(
					StringUtils.stringFromBinary(
							index.getId().getBytes()));
			writer = new DynamoDBWriter(
					tableName,
					client);
			final Boolean tableExists = tableExistsCache.get(
					tableName);
			if (tableExists == null || !tableExists) {
				TableUtils.createTableIfNotExists(
						client,
						new CreateTableRequest()
								.withTableName(
										tableName)
								.withAttributeDefinitions(
										new AttributeDefinition(
												GW_ID_KEY,
												ScalarAttributeType.B),
										new AttributeDefinition(
												GW_IDX_KEY,
												ScalarAttributeType.B))
								.withKeySchema(
										new KeySchemaElement(
												GW_ID_KEY,
												KeyType.HASH),
										new KeySchemaElement(
												GW_IDX_KEY,
												KeyType.RANGE))
								.withProvisionedThroughput(
										new ProvisionedThroughput(
												Long.valueOf(
														10),
												Long.valueOf(
														10))));
				tableExistsCache.put(
						tableName,
						true);
			}
		}
	}

	@Override
	public List<ByteArrayId> writeInternal(
			final T entry,
			final VisibilityWriter<T> visibilityWriter ) {

		DataStoreEntryInfo entryInfo;
		synchronized (this) {

			ensureOpen();
			if (writer == null) {
				return Collections.emptyList();
			}
			entryInfo = DataStoreUtils.getIngestInfo(
					(WritableDataAdapter<T>) adapter,
					index,
					entry,
					DataStoreUtils.UNCONSTRAINED_VISIBILITY);
			if (entryInfo == null) {
				return Collections.EMPTY_LIST;
			}
			writer.write(
					getWriteRequests(
							adapterId,
							entryInfo));
			callback.entryIngested(
					entryInfo,
					entry);
		}
		return entryInfo.getRowIds();
	}

	private static <T> List<WriteRequest> getWriteRequests(
			final byte[] adapterId,
			final DataStoreEntryInfo ingestInfo ) {
		final List<WriteRequest> mutations = new ArrayList<WriteRequest>();
		final List<byte[]> fieldInfoBytesList = new ArrayList<>();
		int totalLength = 0;
		for (final FieldInfo<?> fieldInfo : ingestInfo.getFieldInfo()) {
			final ByteBuffer fieldInfoBytes = ByteBuffer.allocate(
					4 + fieldInfo.getWrittenValue().length);
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
		for (final ByteArrayId insertionId : ingestInfo.getInsertionIds()) {
			final Map<String, AttributeValue> map = new HashMap<String, AttributeValue>();
			final ByteBuffer idBuffer = ByteBuffer.allocate(
					ingestInfo.getDataId().length + adapterId.length + 4);
			idBuffer.putInt(
					ingestInfo.getDataId().length);
			idBuffer.put(
					ingestInfo.getDataId());
			idBuffer.put(
					adapterId);
			map.put(
					GW_ID_KEY,
					new AttributeValue().withB(
							idBuffer));
			if (insertionId.getBytes().length == 0){
				System.err.println("crap");
			}
			map.put(
					GW_IDX_KEY,
					new AttributeValue().withB(
							ByteBuffer.wrap(
									insertionId.getBytes())));
			map.put(
					GW_VALUE_KEY,
					new AttributeValue().withB(
							allFields));
			mutations.add(
					new WriteRequest(
							new PutRequest(
									map)));
		}
		return mutations;
	}

}
