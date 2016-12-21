package mil.nga.giat.geowave.datastore.cassandra.metadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.metadata.AbstractGeowavePersistence;
import mil.nga.giat.geowave.datastore.cassandra.operations.CassandraOperations;

public class AbstractCassandraPersistence<T extends Persistable> extends
		AbstractGeowavePersistence<T>
{
	private static final String PRIMARY_ID_KEY = "I";
	private static final String SECONDARY_ID_KEY = "S";
	private static final String VALUE_KEY = "V";

	private final static Logger LOGGER = Logger.getLogger(
			AbstractCassandraPersistence.class);
	protected static final String[] METADATA_CFS = new String[] {
		CassandraAdapterIndexMappingStore.ADAPTER_INDEX_CF,
		CassandraAdapterStore.ADAPTER_CF,
		CassandraDataStatisticsStore.STATISTICS_CF,
		CassandraIndexStore.INDEX_CF
	};
	protected final CassandraOperations operations;

	public AbstractCassandraPersistence(
			final CassandraOperations operations ) {
		super(
				operations);
		this.operations = operations;
	}

	protected ByteArrayId getSecondaryId(
			final Row row ) {
		final ByteBuffer v = row.getBytes(
				SECONDARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
		}
		return null;
	}

	protected ByteArrayId getPrimaryId(
			final Row row ) {
		final ByteBuffer v = row.getBytes(
				PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
		}
		return null;
	}

	protected ByteArrayId getPersistenceTypeName(
			final Row row ) {
		final ByteBuffer v = row.getBytes(
				PRIMARY_ID_KEY);
		if (v != null) {
			return new ByteArrayId(
					v.array());
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
		final Insert insert = operations.getInsert(getTablename());
		insert.value(
				PRIMARY_ID_KEY,
				ByteBuffer.wrap(
						id.getBytes()));
		if (secondaryId != null) {
			insert.value(
					SECONDARY_ID_KEY,
					ByteBuffer.wrap(
							secondaryId.getBytes()));
		}
		insert.value(
				VALUE_KEY,
				ByteBuffer.wrap(
						PersistenceUtils.toBinary(
								object)));
		operations.getSession().execute(
				insert);
	}

	protected CloseableIterator<T> getAllObjectsWithSecondaryId(
			final ByteArrayId secondaryId,
			final String... authorizations ) {
		final Insert insert = operations.getInsert(getTablename());
		
				if (!operations.tableExists(
						getTablename())) {
					return new CloseableIterator.Wrapper<>(
							Iterators.emptyIterator());
				}
			operations.getSession().execute(QueryBuilder.eq("", ""));
		final ScanResult result = client.scan(
				new ScanRequest(
						dynamodbOperations.getQualifiedTableName(
								getTablename())).addScanFilterEntry(
										SECONDARY_ID_KEY,
										new Condition()
												.withAttributeValueList(
														new AttributeValue().withB(
																ByteBuffer.wrap(
																		secondaryId.getBytes())))
												.withComparisonOperator(
														ComparisonOperator.EQ)));
		return new CloseableIterator.Wrapper<T>(
				Lists.transform(
						result.getItems(),
						new EntryToValueFunction()).iterator());
	}

	@Override
	protected String getTablename() {
		return getPersistenceTypeName() + "_" + super.getTablename();
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
		final List<Row> results = getResults(
				primaryId,
				secondaryId,
				authorizations);
		final Iterator<Row> it = results.iterator();
		if (!it.hasNext()) {
			LOGGER.warn(
					"Object '" + getCombinedId(
							primaryId,
							secondaryId).getString() + "' not found");
			return null;
		}
		final Row entry = it.next();
		return entryToValue(
				entry);
	}

	protected CloseableIterator<T> getObjects(
			final String... authorizations ) {
		final List<Row> results = getFullResults(
				authorizations);
		return new CloseableIterator.Wrapper<T>(
				Lists.transform(
						results,
						new EntryToValueFunction()).iterator());
	}

	@SuppressWarnings("unchecked")
	protected T entryToValue(
			final Row entry ) {
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
		final String tableName = dynamodbOperations.getQualifiedTableName(
				getTablename());
		final Boolean tableExists = tableExistsCache.get(
				tableName);
		if ((tableExists == null) || !tableExists) {
			try {
				if (!dynamodbOperations.tableExists(
						tableName)) {
					return Collections.EMPTY_LIST;
				}
			}
			catch (final IOException e) {
				LOGGER.warn(
						"unable to check table existence",
						e);
				return Collections.EMPTY_LIST;
			}
		}
		if (primaryId != null) {
			final QueryRequest query = new QueryRequest(
					tableName);
			if (secondaryId != null) {
				query.addQueryFilterEntry(
						SECONDARY_ID_KEY,
						new Condition()
								.withAttributeValueList(
										new AttributeValue().withB(
												ByteBuffer.wrap(
														secondaryId.getBytes())))
								.withComparisonOperator(
										ComparisonOperator.EQ));
			}
			query.addKeyConditionsEntry(
					PRIMARY_ID_KEY,
					new Condition()
							.withAttributeValueList(
									new AttributeValue().withB(
											ByteBuffer.wrap(
													primaryId.getBytes())))
							.withComparisonOperator(
									ComparisonOperator.EQ));
			final QueryResult result = client.query(
					query);
			return result.getItems();
		}

		final ScanRequest scan = new ScanRequest(
				tableName);
		// scan.addScanFilterEntry(
		// TYPE_KEY,
		// new Condition().withAttributeValueList(
		// new AttributeValue(
		// getPersistenceTypeName())).withComparisonOperator(ComparisonOperator.EQ));
		if (secondaryId != null) {
			scan.addScanFilterEntry(
					SECONDARY_ID_KEY,
					new Condition()
							.withAttributeValueList(
									new AttributeValue().withB(
											ByteBuffer.wrap(
													secondaryId.getBytes())))
							.withComparisonOperator(
									ComparisonOperator.EQ));
		}
		final ScanResult result = client.scan(
				scan);
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
