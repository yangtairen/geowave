package mil.nga.giat.geowave.datastore.cassandra.operations;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

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
			final String tableName )
			throws IOException {
		return session.getCluster().getMetadata().getKeyspace(
				gwNamespace).getTable(
						tableName) != null;
	}

	public Session getSession() {
		return session;
	}

	public Insert getInsert(
			final String table ) {
		return QueryBuilder.insertInto(
				gwNamespace,
				table);
	}
//	public Query getQuer(){
//		QueryBuilder.eq(name, value)
//	}

	@Override
	public void deleteAll()
			throws Exception {}

}
