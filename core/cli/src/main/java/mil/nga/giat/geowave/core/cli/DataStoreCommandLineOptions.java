package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DataStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<DataStore>
{
	public DataStoreCommandLineOptions(
			final GenericStoreFactory<DataStore> factory,
			final Map<String, Object> configOptions ) {
		super(
				factory,
				configOptions);
	}

	public static void applyOptions(
			final Options allOptions ) {
		applyOptions(
				allOptions,
				new DataStoreCommandLineHelper());
	}

	public static DataStoreCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		return (DataStoreCommandLineOptions) parseOptions(
				commandLine,
				new DataStoreCommandLineHelper());
	}

	@Override
	public DataStore createStore(
			final String namespace ) {
		return GeoWaveStoreFinder.createDataStore(
				configOptions,
				namespace);
	}

	private static class DataStoreCommandLineHelper implements
			CommandLineHelper<DataStore, DataStoreFactorySpi>
	{
		@Override
		public Map<String, DataStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredDataStoreFactories();
		}

		@Override
		public String getOptionName() {
			return "datastore";
		}

		@Override
		public GenericStoreCommandLineOptions<DataStore> createCommandLineOptions(
				final GenericStoreFactory<DataStore> factory,
				final Map<String, Object> configOptions ) {
			return new DataStoreCommandLineOptions(
					factory,
					configOptions);
		}
	}
}
