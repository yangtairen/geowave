package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStoreFactorySpi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DataStatisticsStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<DataStatisticsStore>
{
	public DataStatisticsStoreCommandLineOptions(
			final GenericStoreFactory<DataStatisticsStore> factory,
			final Map<String, Object> configOptions,
			final String namespace ) {
		super(
				factory,
				configOptions,
				namespace);
	}

	public static void applyOptions(
			final Options allOptions ) {
		applyOptions(
				null,
				allOptions);
	}

	public static void applyOptions(
			final String prefix,
			final Options allOptions ) {
		applyOptions(
				prefix,
				allOptions,
				new DataStatisticsStoreCommandLineHelper());
	}

	public static DataStatisticsStoreCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		return parseOptions(
				null,
				commandLine);
	}

	public static DataStatisticsStoreCommandLineOptions parseOptions(
			final String prefix,
			final CommandLine commandLine )
			throws ParseException {
		return (DataStatisticsStoreCommandLineOptions) parseOptions(
				prefix,
				commandLine,
				new DataStatisticsStoreCommandLineHelper());
	}

	@Override
	public DataStatisticsStore createStore() {
		return GeoWaveStoreFinder.createDataStatisticsStore(
				configOptions,
				namespace);
	}

	private static class DataStatisticsStoreCommandLineHelper implements
			CommandLineHelper<DataStatisticsStore, DataStatisticsStoreFactorySpi>
	{
		@Override
		public Map<String, DataStatisticsStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredDataStatisticsStoreFactories();
		}

		@Override
		public String getOptionName() {
			return "statstore";
		}

		@Override
		public GenericStoreCommandLineOptions<DataStatisticsStore> createCommandLineOptions(
				final GenericStoreFactory<DataStatisticsStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new DataStatisticsStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}