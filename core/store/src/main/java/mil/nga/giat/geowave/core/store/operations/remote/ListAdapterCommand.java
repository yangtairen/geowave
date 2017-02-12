package mil.nga.giat.geowave.core.store.operations.remote;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;

@GeowaveOperation(name = "listadapter", parentOperation = RemoteSection.class, restEnabled = GeowaveOperation.RestEnabledType.POST)
@Parameters(commandDescription = "Display all adapters in this remote store")
public class ListAdapterCommand extends
		DefaultOperation<String> implements
		Command
{
	
	private static final Logger LOGGER = LoggerFactory.getLogger(RecalculateStatsCommand.class);

	@Parameter(description = "<store name>")
	private List<String> parameters = new ArrayList<String>();

	private DataStorePluginOptions inputStoreOptions = null;

	@Override
	public void execute(
			OperationParams params ){
		JCommander.getConsole().println(
				"Available adapters: " + computeResults(params));
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String storeName ) {
		this.parameters = new ArrayList<String>();
		this.parameters.add(storeName);
	}

	public DataStorePluginOptions getInputStoreOptions() {
		return inputStoreOptions;
	}

	public void setInputStoreOptions(
			DataStorePluginOptions inputStoreOptions ) {
		this.inputStoreOptions = inputStoreOptions;
	}

	@Override
	protected String computeResults(
			OperationParams params ) {
		if (parameters.size() < 1) {
			throw new ParameterException(
					"Must specify store name");
		}

		String inputStoreName = parameters.get(0);

		// Attempt to load store.
		File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}

		final CloseableIterator<DataAdapter<?>> it = inputStoreOptions.createAdapterStore().getAdapters();
		final StringBuffer buffer = new StringBuffer();
		while (it.hasNext()) {
			DataAdapter<?> adapter = it.next();
			buffer.append(
					adapter.getAdapterId().getString()).append(
					' ');
		}
		try {
			it.close();
		}
		catch (IOException e) {
			LOGGER.error("Unable to close Iterator", 
					e);
		}
		return buffer.toString();
	}
}
