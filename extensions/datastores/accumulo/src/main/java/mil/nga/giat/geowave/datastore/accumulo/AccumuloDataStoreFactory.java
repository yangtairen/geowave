package mil.nga.giat.geowave.datastore.accumulo;

import java.util.Map;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;

public class AccumuloDataStoreFactory extends
		AbstractAccumuloStoreFactory<DataStore> implements
		DataStoreFactorySpi
{

	@Override
	public DataStore createStore(
			final Map<String, Object> configOptions,
			final String namespace ) {
		// TODO also need to add config options for AccumuloOptions
		return new AccumuloDataStore(
				createOperations(
						configOptions,
						namespace));
	}

}
