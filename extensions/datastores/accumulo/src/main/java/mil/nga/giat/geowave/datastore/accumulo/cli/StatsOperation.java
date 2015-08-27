package mil.nga.giat.geowave.datastore.accumulo.cli;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.DataStatisticsStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloOptions;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

/**
 * 
 * Simple command line tool to recalculate statistics for an adapter.
 * 
 */
public abstract class StatsOperation implements
		CLIOperationDriver
{
	protected static final Logger LOGGER = Logger.getLogger(StatsOperation.class);

	public boolean runOperation(
			final DataStore dataStore,
			final AdapterStore adapterStore,
			final IndexStore indexStore,
			final DataStatisticsStore statsStore,
			final ByteArrayId adapterId,
			final String[] authorizations )
			throws IOException {
		final AccumuloOptions accumuloOptions = new AccumuloOptions();
		accumuloOptions.setPersistDataStatistics(true);
		final DataAdapter<?> adapter = null;
		if (adapterId != null) {
			adapterStore.getAdapter(adapterId);
			if (adapter == null) {
				LOGGER.error("Unknown adapter " + adapterId);
				return false;
			}
		}
		return doWork(
				statsStore,
				dataStore,
				indexStore,
				adapter,
				authorizations);
	}

	public abstract boolean doWork(
			DataStatisticsStore statsStore,
			DataStore dataStore,
			IndexStore indexStore,
			DataAdapter<?> adapter,
			String[] authorizations );

	private static String[] getAuthorizations(
			final String auths ) {
		if ((auths == null) || (auths.length() == 0)) {
			return new String[0];
		}
		final String[] authsArray = auths.split(",");
		for (int i = 0; i < authsArray.length; i++) {
			authsArray[i] = authsArray[i].trim();
		}
		return authsArray;
	}

	/** Is an adapter type/id required? */
	protected boolean isTypeRequired() {
		return false;
	}

	@Override
	public void runOperation(
			final String[] args )
			throws ParseException {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		AdapterStoreCommandLineOptions.applyOptions(allOptions);
		DataStatisticsStoreCommandLineOptions.applyOptions(allOptions);
		IndexStoreCommandLineOptions.applyOptions(allOptions);
		StatsCommandLineOptions.applyOptions(
				allOptions,
				isTypeRequired());
		final BasicParser parser = new BasicParser();
		try {
			final CommandLine commandLine = parser.parse(
					allOptions,
					args);
			final DataStoreCommandLineOptions dataStoreOptions = DataStoreCommandLineOptions.parseOptions(commandLine);
			final AdapterStoreCommandLineOptions adapterStoreOptions = AdapterStoreCommandLineOptions.parseOptions(commandLine);
			final DataStatisticsStoreCommandLineOptions dataStatisticsStoreOptions = DataStatisticsStoreCommandLineOptions.parseOptions(commandLine);
			final IndexStoreCommandLineOptions indexStoreOptions = IndexStoreCommandLineOptions.parseOptions(commandLine);

			final StatsCommandLineOptions statsOperations = StatsCommandLineOptions.parseOptions(commandLine);
			runOperation(
					dataStoreOptions.createStore(),
					adapterStoreOptions.createStore(),
					indexStoreOptions.createStore(),
					dataStatisticsStoreOptions.createStore(),
					statsOperations.getTypeName() != null ? new ByteArrayId(
							statsOperations.getTypeName()) : null,
					getAuthorizations(statsOperations.getAuthorizations()));
		}
		catch (final ParseException e) {
			LOGGER.error(
					"Unable to parse stats tool arguments",
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to run stats operation",
					e);
		}
	}

}
