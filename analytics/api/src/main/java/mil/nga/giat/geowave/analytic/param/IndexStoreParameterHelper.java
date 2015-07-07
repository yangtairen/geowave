package mil.nga.giat.geowave.analytic.param;

import java.util.Map;

import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.index.IndexStoreFactorySpi;
import mil.nga.giat.geowave.mapreduce.input.GeoWaveInputFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;

public class IndexStoreParameterHelper implements
		ParameterHelper<IndexStoreCommandLineOptions>
{
	@Override
	public Class<IndexStoreCommandLineOptions> getBaseClass() {
		return IndexStoreCommandLineOptions.class;
	}

	@Override
	public Option[] getOptions() {
		final Options allOptions = new Options();
		IndexStoreCommandLineOptions.applyOptions(allOptions);
		return (Option[]) allOptions.getOptions().toArray(
				new Option[] {});
	}

	@Override
	public IndexStoreCommandLineOptions getValue(
			final CommandLine commandLine )
			throws ParseException {
		return IndexStoreCommandLineOptions.parseOptions(commandLine);
	}

	@Override
	public void setValue(
			final Configuration config,
			final Class<?> scope,
			final IndexStoreCommandLineOptions value ) {
		GeoWaveInputFormat.setIndexStoreName(
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
	public IndexStoreCommandLineOptions getValue(
			final JobContext context,
			final Class<?> scope,
			final IndexStoreCommandLineOptions defaultValue ) {
		final Map<String, String> configOptions = GeoWaveInputFormat.getStoreConfigOptions(context);
		final String indexStoreName = GeoWaveInputFormat.getIndexStoreName(context);
		final String geowaveNamespace = GeoWaveInputFormat.getGeoWaveNamespace(context);
		if ((indexStoreName != null) && (!indexStoreName.isEmpty())) {
			final IndexStoreFactorySpi factory = GeoWaveStoreFinder.getRegisteredIndexStoreFactories().get(
					indexStoreName);
			return new IndexStoreCommandLineOptions(
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
