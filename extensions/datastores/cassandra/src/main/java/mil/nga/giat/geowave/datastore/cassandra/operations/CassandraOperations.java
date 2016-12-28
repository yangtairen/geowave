package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.io.IOException;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;

import mil.nga.giat.geowave.core.store.DataStoreOperations;
import mil.nga.giat.geowave.datastore.cassandra.util.SessionPool;

public class CassandraOperations implements
		DataStoreOperations
{
	private final Session session;
	private final String gwNamespace;

	public CassandraOperations(
			final String contactPoints )
			throws IOException {
		this(
				contactPoints,
				"default");
	}

	public CassandraOperations(
			final String contactPoints,
			final String gwNamespace )
			throws IOException {
		session = SessionPool.getInstance().getSession(
				contactPoints);
		this.gwNamespace = gwNamespace;
	}

	@Override
	public boolean tableExists(
			final String tableName ) {
		return session.getCluster().getMetadata().getKeyspace(
				gwNamespace).getTable(
						tableName) != null;
	}

	public Session getSession() {
		return session;
	}

	public Create getCreateTable(
			final String table ) {
		return SchemaBuilder.createTable(
				gwNamespace,
				table);
	}

	public Insert getInsert(
			final String table ) {
		return QueryBuilder.insertInto(
				gwNamespace,
				table);
	}

	public Select getSelect(
			final String table,
			final String... columns ) {
		return QueryBuilder.select(
				columns).from(
						gwNamespace,
						table);
	}

	@Override
	public void deleteAll()
			throws Exception {}

}
