package mil.nga.giat.geowave.core.store.operations.config;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.restlet.representation.Representation;
import org.restlet.data.Form;
import org.restlet.data.Status;
import org.restlet.resource.Post;
import org.restlet.resource.ServerResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.annotations.RestParameters;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.cli.parser.ManualOperationParams;
import mil.nga.giat.geowave.core.store.GeoWaveStoreFinder;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryRequiredOptions;
import mil.nga.giat.geowave.core.store.memory.MemoryStoreFactoryFamily;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

@GeowaveOperation(name = "addstore", parentOperation = ConfigSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "Create a store within Geowave")
public class AddStoreCommand extends
		DefaultOperation<Void> implements
		Command
{

	private final static Logger LOGGER = LoggerFactory.getLogger(AddStoreCommand.class);

	public static final String PROPERTIES_CONTEXT = "properties";

	@Parameter(description = "<name>")
	@RestParameters(names = {
		"name"
	})
	private List<String> parameters = new ArrayList<String>();

	@Parameter(names = {
		"-d",
		"--default"
	}, description = "Make this the default store in all operations")
	private Boolean makeDefault;

	@Parameter(names = {
		"-t",
		"--type"
	}, required = true, description = "The type of store, such as accumulo, memory, etc")
	private String storeType;

	@ParametersDelegate
	private DataStorePluginOptions pluginOptions = new DataStorePluginOptions();

	@Override
	public boolean prepare(
			OperationParams params ) {

		// Load SPI options for the given type into pluginOptions.
		if (storeType != null) {
			if (storeType.equals("memory")) GeoWaveStoreFinder.getRegisteredStoreFactoryFamilies().put(
					storeType,
					new MemoryStoreFactoryFamily());
			pluginOptions.selectPlugin(storeType);
		}
		else {
			// Try to load the 'default' options.

			File configFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);
			Properties existingProps = ConfigOptions.loadProperties(
					configFile,
					null);

			String defaultStore = existingProps.getProperty(DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE);

			// Load the default index.
			if (defaultStore != null) {
				try {
					if (pluginOptions.load(
							existingProps,
							DataStorePluginOptions.getStoreNamespace(defaultStore))) {
						// Set the required type option.
						this.storeType = pluginOptions.getType();
					}
				}
				catch (ParameterException pe) {
					LOGGER.warn(
							"Couldn't load default store: " + defaultStore,
							pe);
				}
			}
		}

		// Successfully prepared.
		return true;
	}

	@Override
	public void execute(
			OperationParams params ) {
		computeResults(params);
	}

	@Override
	public Void computeResults(
			OperationParams params ) {

		File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

		// Ensure that a name is chosen.
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		// Make sure we're not already in the index.
		DataStorePluginOptions existingOptions = new DataStorePluginOptions();
		if (existingOptions.load(
				existingProps,
				getNamespace())) {
			throw new ParameterException(
					"That store already exists: " + getPluginName());
		}

		// Save the store options.
		pluginOptions.save(
				existingProps,
				getNamespace());

		final StoreFactoryOptions opts = (MemoryRequiredOptions) pluginOptions.getFactoryOptions();
		opts.setGeowaveNamespace("namespace");

		// Make default?
		if (Boolean.TRUE.equals(makeDefault)) {
			existingProps.setProperty(
					DataStorePluginOptions.DEFAULT_PROPERTY_NAMESPACE,
					getPluginName());
		}

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);

		return null;
	}

	public DataStorePluginOptions getPluginOptions() {
		return pluginOptions;
	}

	public String getPluginName() {
		return parameters.get(0);
	}

	public String getNamespace() {
		return DataStorePluginOptions.getStoreNamespace(getPluginName());
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public Boolean getMakeDefault() {
		return makeDefault;
	}

	public void setMakeDefault(
			Boolean makeDefault ) {
		this.makeDefault = makeDefault;
	}

	public String getStoreType() {
		return storeType;
	}

	public void setStoreType(
			String storeType ) {
		this.storeType = storeType;
	}

	public void setPluginOptions(
			DataStorePluginOptions pluginOptions ) {
		this.pluginOptions = pluginOptions;
	}
}
