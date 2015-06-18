package mil.nga.giat.geowave.analytic.param;

import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
	public Option[] getOptions() {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public DataStoreCommandLineOptions getValue(
			final CommandLine commandLine )
			throws ParseException {
		return DataStoreCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final DataStoreCommandLineOptions value ) {
		GeoWaveInputFormat.setDataStoreName(
				config,
				value.getFactory().getName());
		GeoWaveInputFormat.setStoreConfigOptions(
				config,
				ConfigUtils.valuesToStrings(
						value.getConfigOptions(),
						value.getFactory().getOptions()));
		GeoWaveInputFormat.setGeoWaveNamespace(
				config,
				value.getNamespace());
	}

	@Override
	public DataStoreCommandLineOptions getValue(
			final JobContext context,
			final Class<?> scope,
			final DataStoreCommandLineOptions defaultValue ) {

		GeoWaveInputFormat.getStoreConfigOptions(context);
		GeoWaveInputFormat.getDataStoreName(context);
		GeoWaveInputFormat.setStoreConfigOptions(
				config,
				ConfigUtils.valuesToStrings(
						value.getConfigOptions(),
						value.getFactory().getOptions()));
		GeoWaveInputFormat.setGeoWaveNamespace(
				config,
				value.getNamespace());
		return null;
	}

}
