package mil.nga.giat.geowave.datastore.hbase.operations.config;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.base.BaseDataStoreOptions;

public class HBaseRequiredOptions extends
		StoreFactoryOptions
{

	public static final String ZOOKEEPER_CONFIG_KEY = "zookeeper";

	@Parameter(names = {
		"-z",
		"--" + ZOOKEEPER_CONFIG_KEY
	}, description = "A comma-separated list of zookeeper servers that an Accumulo instance is using", required = true)
	private String zookeeper;

	@ParametersDelegate
	private BaseDataStoreOptions additionalOptions = new BaseDataStoreOptions();

	public String getZookeeper() {
		return zookeeper;
	}

	public void setZookeeper(
			final String zookeeper ) {
		this.zookeeper = zookeeper;
	}

	public BaseDataStoreOptions getAdditionalOptions() {
		return additionalOptions;
	}

	public void setAdditionalOptions(
			final BaseDataStoreOptions additionalOptions ) {
		this.additionalOptions = additionalOptions;
	}
}
