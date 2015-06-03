package mil.nga.giat.geowave.core.cli;

import java.util.Map;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStoreFactorySpi;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class AdapterStoreCommandLineOptions extends
		GenericStoreCommandLineOptions<AdapterStore>
{
	public AdapterStoreCommandLineOptions(
			final GenericStoreFactory<AdapterStore> factory,
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
				allOptions,
				new AdapterStoreCommandLineHelper());
	}

	public static AdapterStoreCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		return (AdapterStoreCommandLineOptions) parseOptions(
				commandLine,
				new AdapterStoreCommandLineHelper());
	}

	@Override
	public AdapterStore createStore() {
		return GeoWaveStoreFinder.createAdapterStore(
				configOptions,
				namespace);
	}

	private static class AdapterStoreCommandLineHelper implements
			CommandLineHelper<AdapterStore, AdapterStoreFactorySpi>
	{
		@Override
		public Map<String, AdapterStoreFactorySpi> getRegisteredFactories() {
			return GeoWaveStoreFinder.getRegisteredAdapterStoreFactories();
		}

		@Override
		public String getOptionName() {
			return "adapterstore";
		}

		@Override
		public GenericStoreCommandLineOptions<AdapterStore> createCommandLineOptions(
				final GenericStoreFactory<AdapterStore> factory,
				final Map<String, Object> configOptions,
				final String namespace ) {
			return new AdapterStoreCommandLineOptions(
					factory,
					configOptions,
					namespace);
		}
	}
}
