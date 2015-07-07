package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.DataStatisticsStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public class DataStatisticsStoreParameterHelper implements
		ParameterHelper<DataStatisticsStoreCommandLineOptions>
{
	@Override
	public Class<DataStatisticsStoreCommandLineOptions> getBaseClass() {
		return DataStatisticsStoreCommandLineOptions.class;
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		DataStatisticsStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public DataStatisticsStoreCommandLineOptions getValue(
			final CommandLine commandLine )
			throws ParseException {
		return DataStatisticsStoreCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final DataStatisticsStoreCommandLineOptions value ) {
		GeoWaveInputFormat.setDataStatisticsStoreName(
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
	public DataStatisticsStoreCommandLineOptions getValue(
			final JobContext context,
			final Class<?> scope,
			final DataStatisticsStoreCommandLineOptions defaultValue ) {
		final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
		final String dataStatisticsStoreName = GeoWaveInputFormat.getDataStatisticsStoreName(context);
		final String geowaveNamespace = GeoWaveInputFormat.getGeoWaveNamespace(context);
		if ((dataStatisticsStoreName != null) && (!dataStatisticsStoreName.isEmpty())) {
			final DataStatisticsStoreFactorySpi factory = GeoWaveStoreFinder.getRegisteredDataStatisticsStoreFactories().get(
					dataStatisticsStoreName);
			return new DataStatisticsStoreCommandLineOptions(
					factory,
					ConfigUtils.valuesFromStrings(
							configOptions,
							factory.getOptions()),
					geowaveNamespace);
		}
		else {
			return null;
		}
	}
}
