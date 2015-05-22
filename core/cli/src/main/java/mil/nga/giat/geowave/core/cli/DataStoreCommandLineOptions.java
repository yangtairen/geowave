package mil.nga.giat.geowave.core.cli;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.DataStoreFactorySpi;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStoreCommandLineOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(DataStoreCommandLineOptions.class);

	private final DataStoreFactorySpi factory;
	private final Map<String, Object> configOptions;

	public DataStoreCommandLineOptions(
			final DataStoreFactorySpi factory,
			final Map<String, Object> configOptions ) {
		this.factory = factory;
		this.configOptions = configOptions;
	}

	public DataStore createDataStore(
			final String namespace ) {
		return GeoWaveStoreFinder.createDataStore(
				configOptions,
				namespace);
	}

	public DataStoreFactorySpi getFactory() {
		return factory;
	}

	public Map<String, Object> getConfigOptions() {
		return configOptions;
	}

	private static Options storeOptionsToCliOptions(
			final AbstractConfigOption<?>[] storeOptions ) {
		final Options cliOptions = new Options();
		for (final AbstractConfigOption<?> storeOption : storeOptions) {
			cliOptions.addOption(storeOptionToCliOption(storeOption));
		}
		return cliOptions;
	}

	private static Option storeOptionToCliOption(
			final AbstractConfigOption<?> storeOption ) {
		final Class<?> cls = GenericTypeResolver.resolveTypeArgument(
				storeOption.getClass(),
				AbstractConfigOption.class);
		final boolean isBoolean = Boolean.class.isAssignableFrom(cls);
		final Option cliOption = new Option(
				ConfigUtils.cleanOptionName(storeOption.getName()),
				isBoolean,
				storeOption.getDescription());
		cliOption.setRequired(!storeOption.isOptional() && !isBoolean);
		return cliOption;
	}

	public static DataStoreCommandLineOptions parseOptions(
			final CommandLine commandLine )
			throws ParseException {
		if (commandLine.hasOption("datastore")) {
			// if data store is given, make sure the commandline options
			// properly match the options for this data store
			final String selectedDataStoreName = commandLine.getOptionValue("datastore");
			final DataStoreFactorySpi selectedDataStoreFactory = GeoWaveStoreFinder.getRegisteredDataStoreFactories().get(
					selectedDataStoreName);
			if (selectedDataStoreFactory == null) {
				final String errorMsg = "Cannot find selected data store '" + selectedDataStoreName + "'";
				LOGGER.error(errorMsg);
				throw new ParseException(
						errorMsg);
			}
			Map<String, Object> configOptions;
			try {
				configOptions = getConfigOptionsForDataStoreFactory(
						commandLine.getArgs(),
						selectedDataStoreFactory);
				return new DataStoreCommandLineOptions(
						selectedDataStoreFactory,
						configOptions);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to parse config options for datastore '" + selectedDataStoreName + "'",
						e);
				throw new ParseException(
						"Unable to parse config options for datastore '" + selectedDataStoreName + "'; " + e.getMessage());
			}
		}
		// if data store is not given, go through all available data stores
		// until one matches the config options
		final Map<String, DataStoreFactorySpi> factories = GeoWaveStoreFinder.getRegisteredDataStoreFactories();
		final String[] additionalArgs = commandLine.getArgs();
		final Map<String, Exception> exceptionsPerDataStoreFactory = new HashMap<String, Exception>();
		for (final Entry<String, DataStoreFactorySpi> factoryEntry : factories.entrySet()) {
			final Map<String, Object> configOptions;
			try {
				configOptions = getConfigOptionsForDataStoreFactory(
						additionalArgs,
						factoryEntry.getValue());
				return new DataStoreCommandLineOptions(
						factoryEntry.getValue(),
						configOptions);
			}
			catch (final Exception e) {
				// it just means this data store is not compatible with the
				// options, add it to a list and we'll log it only if no data
				// store is compatible
				exceptionsPerDataStoreFactory.put(
						factoryEntry.getKey(),
						e);
			}
		}
		// just log all the exceptions so that it is apparent where the
		// commandline incompatibility might be
		for (final Entry<String, Exception> exceptionEntry : exceptionsPerDataStoreFactory.entrySet()) {
			LOGGER.error(
					"Could not parse commandline for datastore '" + exceptionEntry.getKey() + "'",
					exceptionEntry.getValue());
		}
		throw new ParseException(
				"No compatible datastore found");
	}

	private static Map<String, Object> getConfigOptionsForDataStoreFactory(
			final String[] additionalArgs,
			final DataStoreFactorySpi dataStoreFactory )
			throws Exception {
		final AbstractConfigOption<?>[] storeOptions = dataStoreFactory.getOptions();
		final Options cliOptions = storeOptionsToCliOptions(storeOptions);

		final BasicParser parser = new BasicParser();
		// parse the datastore options
		final CommandLine dataStoreCommandLine = parser.parse(
				cliOptions,
				additionalArgs);
		final Map<String, Object> configOptions = new HashMap<String, Object>();
		for (final AbstractConfigOption<?> option : storeOptions) {
			final String cliOptionName = ConfigUtils.cleanOptionName(option.getName());
			final Class<?> cls = GenericTypeResolver.resolveTypeArgument(
					option.getClass(),
					AbstractConfigOption.class);
			final boolean isBoolean = Boolean.class.isAssignableFrom(cls);
			final boolean hasOption = dataStoreCommandLine.hasOption(cliOptionName);
			if (isBoolean) {
				configOptions.put(
						option.getName(),
						option.valueFromString(hasOption ? "true" : "false"));
			}
			else if (hasOption) {
				final String optionValueStr = dataStoreCommandLine.getOptionValue(cliOptionName);
				configOptions.put(
						option.getName(),
						option.valueFromString(optionValueStr));
			}
		}
		return configOptions;
	}

	public static void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(new Option(
				"datastore",
				true,
				"Explicitly set the datastore by name, if not set, a datastore will be used if all of its required options are provided. " + ConfigUtils.getOptions(
						GeoWaveStoreFinder.getRegisteredDataStoreFactories().keySet(),
						"Available datastores: ")));
	}
}
