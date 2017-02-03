package mil.nga.giat.geowave.core.store.base;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.DataStoreOperations;

public class MockDataStoreOperations implements
		DataStoreOperations
{
	public String tableNamespace;

	public MockDataStoreOperations(
			final String tableNamespace ) {
		this.tableNamespace = tableNamespace;
	}

	@Override
	public boolean tableExists(
			final String tableName )
			throws IOException {
		return false;
	}

	@Override
	public void deleteAll()
			throws Exception {}

	@Override
	public String getTableNameSpace() {
		return tableNamespace;
	}

}
