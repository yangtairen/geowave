package mil.nga.giat.geowave.core.store.memory;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;

abstract public class AbstractMemoryStoreFactory<T> implements
		GenericStoreFactory<T>
{
	@Override
	public String getName() {
		return "memory";
	}

	@Override
	public String getDescription() {
		return "A GeoWave store that is in memory typically only used for test purposes";
	}

	@Override
	public AbstractConfigOption<?>[] getOptions() {
		return new AbstractConfigOption<?>[] {};
	}
}
