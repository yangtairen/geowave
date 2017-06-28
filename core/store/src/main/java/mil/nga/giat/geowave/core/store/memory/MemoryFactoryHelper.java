package mil.nga.giat.geowave.core.store.memory;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericFactory;
import mil.nga.giat.geowave.core.store.StoreFactoryHelper;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.operations.DataStoreOperations;

public class MemoryFactoryHelper implements
		StoreFactoryHelper
{


	private static final Map<String,DataStoreOperations> STORE_CACHE = new HashMap<String,DataStoreOperations>();
	/**
	 * Return the default options instance. This is actually a method that
	 * should be implemented by the individual factories, but is placed here
	 * since it's the same.
	 * 
	 * @return
	 */
	public StoreFactoryOptions createOptionsInstance() {
		return new MemoryRequiredOptions();
	}

	@Override
	public DataStoreOperations createOperations(
			StoreFactoryOptions options ) {
		
		return new MemoryDataStoreOperations();
	}
}
