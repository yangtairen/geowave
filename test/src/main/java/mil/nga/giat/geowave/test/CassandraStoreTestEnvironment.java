package mil.nga.giat.geowave.test;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseDataStoreFactory;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

public class CassandraStoreTestEnvironment extends
		StoreTestEnvironment
{
	private static final GenericStoreFactory<DataStore> STORE_FACTORY = new HBaseDataStoreFactory();
	private static CassandraStoreTestEnvironment singletonInstance = null;

	public static synchronized CassandraStoreTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new CassandraStoreTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = Logger.getLogger(
			CassandraStoreTestEnvironment.class);

	private CassandraStoreTestEnvironment() {}

	@Override
	protected void initOptions(
			final StoreFactoryOptions options ) {}

	@Override
	protected GenericStoreFactory<DataStore> getDataStoreFactory() {
		return STORE_FACTORY;
	}

	@Override
	public void setup() {
		try {
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
		}
		catch (ConfigurationException | TTransportException | IOException | InterruptedException e) {
			LOGGER.error(
					"Failed to start embedded Cassandra server",
					e);
		}
	}

	@Override
	public void tearDown() {
		try {
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		}
		catch (final Exception e) {
			LOGGER.warn(
					"Unable to clear embedded cassandra server",
					e);
		}
	}

	@Override
	protected GeoWaveStoreType getStoreType() {
		return GeoWaveStoreType.CASSANDRA;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}
}
