package mil.nga.giat.geowave.core.cli;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.store.GenericStoreFactory;
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

abstract public class GenericStoreCommandLineOptions<T>
{
	private final static Logger LOGGER = LoggerFactory.getLogger(GenericStoreCommandLineOptions.class);

	protected final GenericStoreFactory<T> factory;
	protected final Map<String, Object> configOptions;
	protected final String namespace;

	public GenericStoreCommandLineOptions(
			final GenericStoreFactory<T> factory,
			final Map<String, Object> configOptions,
			final String namespace ) {
		this.factory = factory;
		this.configOptions = configOptions;
		this.namespace = namespace;
	}

	abstract public T createStore();

	public GenericStoreFactory<T> getFactory() {
		return factory;
	}

	public Map<String, Object> getConfigOptions() {
		return configOptions;
	}

	public String getNamespace() {
		return namespace;
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

	protected static Map<String, Object> getConfigOptionsForStoreFactory(
			final String[] additionalArgs,
			final GenericStoreFactory<?> genericStoreFactory )
			throws Exception {
		final AbstractConfigOption<?>[] storeOptions = genericStoreFactory.getOptions();
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

	public static <T, F extends GenericStoreFactory<T>> void applyOptions(
			final Options allOptions,
			final CommandLineHelper<T, F> helper ) {
		final String optionName = helper.getOptionName();
		allOptions.addOption(new Option(
				optionName,
				true,
				"Explicitly set the " + optionName + " by name, if not set, an " + optionName + " will be used if all of its required options are provided. " + ConfigUtils.getOptions(
						helper.getRegisteredFactories().keySet(),
						"Available " + optionName + "s: ")));
		final Option namespace = new Option(
				"n",
				"namespace",
				true,
				"The geowave namespace (optional; default is no namespace)");
		namespace.setRequired(false);
		allOptions.addOption(namespace);
	}

	public static <T, F extends GenericStoreFactory<T>> GenericStoreCommandLineOptions<T> parseOptions(
			final CommandLine commandLine,
			final CommandLineHelper<T, F> helper )
			throws ParseException {
		final String optionName = helper.getOptionName();
		final String namespace = commandLine.getOptionValue(
				"n",
				"");
		if (commandLine.hasOption(optionName)) {
			// if data store is given, make sure the commandline options
			// properly match the options for this store
			final String selectedStoreName = commandLine.getOptionValue(optionName);
			final F selectedStoreFactory = helper.getRegisteredFactories().get(
					selectedStoreName);
			if (selectedStoreFactory == null) {
				final String errorMsg = "Cannot find selected " + optionName + " '" + selectedStoreFactory + "'";
				LOGGER.error(errorMsg);
				throw new ParseException(
						errorMsg);
			}
			Map<String, Object> configOptions;
			try {
				configOptions = getConfigOptionsForStoreFactory(
						commandLine.getArgs(),
						selectedStoreFactory);
				return helper.createCommandLineOptions(
						selectedStoreFactory,
						configOptions,
						namespace);
			}
			catch (final Exception e) {
				LOGGER.error(
						"Unable to parse config options for " + optionName + " '" + selectedStoreName + "'",
						e);
				throw new ParseException(
						"Unable to parse config options for  " + optionName + " '" + selectedStoreName + "'; " + e.getMessage());
			}
		}
		// if data store is not given, go through all available data stores
		// until one matches the config options
		final Map<String, F> factories = helper.getRegisteredFactories();
		final String[] additionalArgs = commandLine.getArgs();
		final Map<String, Exception> exceptionsPerDataStoreFactory = new HashMap<String, Exception>();
		for (final Entry<String, F> factoryEntry : factories.entrySet()) {
			final Map<String, Object> configOptions;
			try {
				configOptions = getConfigOptionsForStoreFactory(
						additionalArgs,
						factoryEntry.getValue());
				return helper.createCommandLineOptions(
						factoryEntry.getValue(),
						configOptions,
						namespace);
			}
			catch (final Exception e) {
				// it just means this store is not compatible with the
				// options, add it to a list and we'll log it only if no store
				// is compatible
				exceptionsPerDataStoreFactory.put(
						factoryEntry.getKey(),
						e);
			}
		}
		// just log all the exceptions so that it is apparent where the
		// commandline incompatibility might be
		for (final Entry<String, Exception> exceptionEntry : exceptionsPerDataStoreFactory.entrySet()) {
			LOGGER.error(
					"Could not parse commandline for " + optionName + " '" + exceptionEntry.getKey() + "'",
					exceptionEntry.getValue());
		}
		throw new ParseException(
				"No compatible " + optionName + " found");
	}

	protected static interface CommandLineHelper<T, F extends GenericStoreFactory<T>>
	{
		public Map<String, F> getRegisteredFactories();

		public String getOptionName();

		public GenericStoreCommandLineOptions<T> createCommandLineOptions(
				GenericStoreFactory<T> factory,
				Map<String, Object> configOptions,
				String namespace );
	}
}
