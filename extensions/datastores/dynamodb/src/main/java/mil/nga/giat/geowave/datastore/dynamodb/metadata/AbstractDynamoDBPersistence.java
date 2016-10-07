package mil.nga.giat.geowave.datastore.dynamodb.metadata;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBAsyncClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.google.common.base.Function;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeowavePersistence;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOperations;

public abstract class AbstractDynamoDBPersistence<T extends Persistable> extends
		AbstractGeowavePersistence<T>
{
	private final static Logger LOGGER = Logger.getLogger(
			AbstractDynamoDBPersistence.class);
	private static final String PRIMARY_ID_KEY = "I";
	private static final String TYPE_KEY = "T";
	private static final String SECONDARY_ID_KEY = "S";
	private static final String VALUE_KEY = "V";

	private final AmazonDynamoDBAsyncClient client;

	public AbstractDynamoDBPersistence(
			final DynamoDBOperations ops) {
		super(
				ops);
		this.client = ops.getClient();
	}

	protected ByteArrayId getSecondaryId(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(
				SECONDARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.getB().array());
		}
		return null;
	}

	protected ByteArrayId getPrimaryId(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(
				PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.getB().array());
		}
		return null;
	}

	protected ByteArrayId getPersistenceTypeName(
			final Map<String, AttributeValue> map ) {
		final AttributeValue v = map.get(
				PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.getB().array());
		}
		return null;
	}

	@Override
	protected void addObject(
			final ByteArrayId id,
			final ByteArrayId secondaryId,
			final T object ) {
		addObjectToCache(
				id,
				secondaryId,
				object);

		final Map<String, AttributeValue> map = new HashMap<>();
		map.put(
				PRIMARY_ID_KEY,
				new AttributeValue().withB(
						ByteBuffer.wrap(
								id.getBytes())));
		if (secondaryId != null) {
			map.put(
					SECONDARY_ID_KEY,
					new AttributeValue().withB(
							ByteBuffer.wrap(
									secondaryId.getBytes())));
		}
		map.put(
				TYPE_KEY,
				new AttributeValue(
						getPersistenceTypeName()));
		map.put(
				VALUE_KEY,
				new AttributeValue().withB(
						ByteBuffer.wrap(
								PersistenceUtils.toBinary(
										object))));
		client.putItem(
				new PutItemRequest(
						getTableName(),
						map));
	}

	protected String getTableName() {
		return operations.getTableNameSpace() + "_" + METADATA_TABLE;
	}

	protected CloseableIterator<T> getAllObjectsWithSecondaryId(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final QueryResult result = client.query(
				new QueryRequest(
						getTablename())
								.addQueryFilterEntry(
										TYPE_KEY,
										new Condition().withAttributeValueList(
												new AttributeValue(
														getPersistenceTypeName())))
								.addQueryFilterEntry(
										SECONDARY_ID_KEY,
										new Condition().withAttributeValueList(
												new AttributeValue().withB(
														ByteBuffer.wrap(
																secondaryId.getBytes())))));
		return new CloseableIterator.Wrapper<T>(
				Lists.transform(
						result.getItems(),
						new EntryToValueFunction()).iterator());
	}

	@SuppressWarnings("unchecked")
	protected T getObject(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final Object cacheResult = getObjectFromCache(
				primaryId,
				secondaryId);
		if (cacheResult != null) {
			return (T) cacheResult;
		}
		final List<Map<String, AttributeValue>> results = getResults(
				primaryId,
				secondaryId,
				authorizations);
		final Iterator<Map<String, AttributeValue>> it = results.iterator();
		if (!it.hasNext()) {
			LOGGER.warn(
					"Object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "' not found");
			return null;
		}
		final Map<String, AttributeValue> entry = it.next();
		return entryToValue(
				entry);
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		final List<Map<String, AttributeValue>> results = getFullResults(
				authorizations);
		return new CloseableIterator.Wrapper<T>(
				Lists.transform(
						results,
						new EntryToValueFunction()).iterator());
	}

	@SuppressWarnings("unchecked")
	protected T entryToValue(
			final Map<String, AttributeValue> entry ) {
		final T result = (T) PersistenceUtils.fromBinary(
				entry.get(
						VALUE_KEY).getB().array(),
				Persistable.class);
		if (result != null) {
			addObjectToCache(
					getPrimaryId(
							result),
					getSecondaryId(
							result),
					result);
		}
		return result;
	}

	private List<Map<String, AttributeValue>> getFullResults(
			final String... authorizations ) {
		return getResults(
				null,
				null,
				authorizations);
	}

	protected List<Map<String, AttributeValue>> getResults(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final QueryRequest query = new QueryRequest(
				getTableName());
		query.addQueryFilterEntry(
				TYPE_KEY,
				new Condition().withAttributeValueList(
						new AttributeValue(
								getPersistenceTypeName())));
		if (secondaryId != null) {
			query.addQueryFilterEntry(
					SECONDARY_ID_KEY,
					new Condition().withAttributeValueList(
							new AttributeValue().withB(
									ByteBuffer.wrap(
											secondaryId.getBytes()))));
		}
		if (primaryId != null) {
			query.addQueryFilterEntry(
					PRIMARY_ID_KEY,
					new Condition().withAttributeValueList(
							new AttributeValue().withB(
									ByteBuffer.wrap(
											primaryId.getBytes()))));
		}
		final QueryResult result = client.query(
				query);
		return result.getItems();
	}

	public boolean deleteObjects(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		return deleteObjects(
				null,
				secondaryId,
				authorizations);
	}

	@Override
	public boolean deleteObjects(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		// TODO

		return true;
	}

	protected boolean objectExists(
			final ByteArrayId primaryId,
			final ByteArrayId secondaryId ) {
		if (getObjectFromCache(
				primaryId,
				secondaryId) != null) {
			return true;
		}
		final List<Map<String, AttributeValue>> results = getResults(
				primaryId,
				secondaryId);

		final Iterator<Map<String, AttributeValue>> it = results.iterator();
		if (it.hasNext()) {
			// may as well cache the result
			return (entryToValue(
					it.next()) != null);
		}
		else {
			return false;
		}

	}

	private class EntryToValueFunction implements
			Function<Map<String, AttributeValue>, T>
	{

		@Override
		public T apply(
				final Map<String, AttributeValue> entry ) {
			return entryToValue(
					entry);
		}

	}
}
