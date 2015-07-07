package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public class AdapterStoreParameterHelper implements
		ParameterHelper<AdapterStoreCommandLineOptions>
{
	@Override
	public Class<AdapterStoreCommandLineOptions> getBaseClass() {
		return AdapterStoreCommandLineOptions.class;
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		AdapterStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public AdapterStoreCommandLineOptions getValue(
			final CommandLine commandLine )
			throws ParseException {
		return AdapterStoreCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final AdapterStoreCommandLineOptions value ) {
		GeoWaveInputFormat.setAdapterStoreName(
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
	public AdapterStoreCommandLineOptions getValue(
			final JobContext context,
			final Class<?> scope,
			final AdapterStoreCommandLineOptions defaultValue ) {
		final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
		final String adapterStoreName = GeoWaveInputFormat.getAdapterStoreName(context);
		final String geowaveNamespace = GeoWaveInputFormat.getGeoWaveNamespace(context);
		if ((adapterStoreName != null) && (!adapterStoreName.isEmpty())) {
			final AdapterStoreFactorySpi factory = GeoWaveStoreFinder.getRegisteredAdapterStoreFactories().get(
					adapterStoreName);
			return new AdapterStoreCommandLineOptions(
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
