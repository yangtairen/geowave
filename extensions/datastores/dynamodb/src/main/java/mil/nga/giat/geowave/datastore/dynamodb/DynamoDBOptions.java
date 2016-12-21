package mil.nga.giat.geowave.datastore.dynamodb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;
import mil.nga.giat.geowave.core.store.DataStoreOptions;
import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;

public class DynamoDBOptions extends
		StoreFactoryOptions
{
	@Parameter(names = "--endpoint")
	protected String endpoint;

	@ParametersDelegate
	protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions();

	public void setEndpoint(
			final String endpoint ) {
		this.endpoint = endpoint;
	}

	public String getEndpoint() {
		return endpoint;
	}

	public DataStoreOptions getBaseOptions() {
		return baseOptions;
	}

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new DynamoDBStoreFactoryFamily();
	}

}
