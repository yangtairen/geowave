package mil.nga.giat.geowave.test;

import org.apache.log4j.Logger;

import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBDataStoreFactory;
import mil.nga.giat.geowave.datastore.dynamodb.DynamoDBOptions;

public class DynamoDBTestEnvironment implements
		StoreTestEnvironment
{
	private static DynamoDBTestEnvironment singletonInstance = null;

	public static synchronized DynamoDBTestEnvironment getInstance() {
		if (singletonInstance == null) {
			singletonInstance = new DynamoDBTestEnvironment();
		}
		return singletonInstance;
	}

	private final static Logger LOGGER = Logger.getLogger(
			AccumuloStoreTestEnvironment.class);

	private DynamoDBTestEnvironment() {}

	@Override
	public void setup() {}

	@Override
	public void tearDown() {}

	@Override
	public DataStorePluginOptions getDataStoreOptions(
			final String namespace ) {
		final DataStorePluginOptions pluginOptions = new DataStorePluginOptions();
		final DynamoDBOptions opts = new DynamoDBOptions();
		opts.setGeowaveNamespace(
				namespace);
		opts.setEndpoint(
				"http://localhost:8000");
		pluginOptions.selectPlugin(
				new DynamoDBDataStoreFactory().getName());
		pluginOptions.setFactoryOptions(
				opts);
		return pluginOptions;
	}

	@Override
	public TestEnvironment[] getDependentEnvironments() {
		return new TestEnvironment[] {};
	}

}
