package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.mapreduce.GeoWaveConfiguratorBase.GeneralConfig;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public class DataStoreParameterHelper implements
		ParameterHelper<DataStoreCommandLineOptions>
{
	@Override
	public Class<DataStoreCommandLineOptions> getBaseClass() {
		return DataStoreCommandLineOptions.class;
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
		DataStoreCommandLineOptions.applyOptions(
				allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public DataStoreCommandLineOptions getValue(
			CommandLine commandline ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setValue(
			Configuration config,
			Class<?> scope,
			DataStoreCommandLineOptions value ) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataStoreCommandLineOptions getValue(
			JobContext context,
			Class<?> scope,
			DataStoreCommandLineOptions defaultValue ) {
		// TODO Auto-generated method stub
		return null;
	}


}
