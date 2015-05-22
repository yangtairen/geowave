package mil.nga.giat.geowave.core.store;

import java.util.Map;

import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;

public interface GenericStoreFactory<T>
{
	public T createStore(
			Map<String, Object> configOptions,
			String namespace );

	public String getName();

	public String getDescription();

	public AbstractConfigOption<?>[] getOptions();
}
