package mil.nga.giat.geowave.datastore.cassandra.metadata;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.datastore.cassandra.AbstractCassandraStoreFactory;
import mil.nga.giat.geowave.datastore.cassandra.operations.config.CassandraRequiredOptions;

public class CassandraAdapterStoreFactory extends
		AbstractCassandraStoreFactory<AdapterStore>
{

	@Override
	public AdapterStore createStore(
			final StoreFactoryOptions options ) {
		if (!(options instanceof CassandraRequiredOptions)) {
			throw new AssertionError(
					"Expected " + CassandraRequiredOptions.class.getSimpleName());
		}
		final CassandraRequiredOptions opts = (CassandraRequiredOptions) options;
		return null;
	}

}