package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase.GeneralConfig;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;

public class DataStoreParameterHelper implements
		ParameterHelper<DataStore>
{
	@Override
	public Class<DataStore> getBaseClass() {
		return DataStore.class;
	}

	@Override
	public void setParameter(
			final Configuration jobConfig,
			final Class<?> jobScope,
			final PropertyManagement propertyValues ) {
		GeoWaveConfiguratorBase.setDataStoreName(
				jobScope,
				jobConfig,
				dataStoreFactory.getName());
		GeoWaveConfiguratorBase.setStoreConfigOptions(
				jobScope,
				jobConfig,
				ConfigUtils.valuesToStrings(
						configOptions,
						dataStoreFactory.getOptions()));
		GeoWaveConfiguratorBase.setGeoWaveNamespace(
				jobScope,
				jobConfig,
				namespace);
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public DataStore getValue(
			final ConfigurationWrapper config,
			final DataStore defaultValue ) {
		
		config.get
		return null;
	}

}
