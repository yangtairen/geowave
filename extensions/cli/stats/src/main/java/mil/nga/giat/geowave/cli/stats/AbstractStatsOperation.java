package mil.nga.giat.geowave.cli.stats;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStatisticsStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.index.IndexStore;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

abstract public class AbstractStatsOperation implements
		CLIOperationDriver
{
	private static final Logger LOGGER = Logger.getLogger(AbstractStatsOperation.class);

	abstract protected boolean calculateStatistics(
			final DataStore dataStore,
			final IndexStore indexStore,
			final DataStatisticsStore statsStore,
			final DataAdapter<?> adapter,
			final String[] authorizations )
			throws IOException;

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

	@Override
	public void runOperation(
			final String[] args )
			throws ParseException {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		AdapterStoreCommandLineOptions.applyOptions(allOptions);
		IndexStoreCommandLineOptions.applyOptions(allOptions);
		DataStatisticsStoreCommandLineOptions.applyOptions(allOptions);
		StatsCommandLineOptions.applyOptions(allOptions);
		try {
			CommandLine commandLine = null;
			CommandLineResult<DataStoreCommandLineOptions> dataStoreCli = null;
			CommandLineResult<AdapterStoreCommandLineOptions> adapterStoreCli = null;
			CommandLineResult<IndexStoreCommandLineOptions> indexStoreCli = null;
			CommandLineResult<DataStatisticsStoreCommandLineOptions> statsStoreCli = null;
			StatsCommandLineOptions statsOperations = null;
			ParseException parseException = null;
			boolean newCommandLine = false;
			do {
				newCommandLine = false;
				dataStoreCli = null;
				adapterStoreCli = null;
				indexStoreCli = null;
				statsStoreCli = null;
				parseException = null;
				try {
					dataStoreCli = DataStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((dataStoreCli != null) && dataStoreCli.isCommandLineChange()) {
					commandLine = dataStoreCli.getCommandLine();
				}
				try {
					adapterStoreCli = AdapterStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((adapterStoreCli != null) && adapterStoreCli.isCommandLineChange()) {
					commandLine = adapterStoreCli.getCommandLine();
					newCommandLine = true;
					continue;
				}
				try {
					indexStoreCli = IndexStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((indexStoreCli != null) && indexStoreCli.isCommandLineChange()) {
					commandLine = indexStoreCli.getCommandLine();
					newCommandLine = true;
					continue;
				}
				try {
					statsStoreCli = DataStatisticsStoreCommandLineOptions.parseOptions(
							allOptions,
							commandLine);
				}
				catch (final ParseException e) {
					parseException = e;
				}
				if ((statsStoreCli != null) && statsStoreCli.isCommandLineChange()) {
					commandLine = statsStoreCli.getCommandLine();
					newCommandLine = true;
					continue;
				}
				statsOperations = StatsCommandLineOptions.parseOptions(commandLine);
			}
			while (newCommandLine);
			if (parseException != null) {
				throw parseException;
			}
			final ByteArrayId adapterId = new ByteArrayId(
					statsOperations.getTypeName());
			final AdapterStore adapterStore = adapterStoreCli.getResult().createStore();
			final DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
			if (adapter == null) {
				LOGGER.error("Unknown adapter " + adapterId);
				System.exit(-1);
			}
			System.exit(calculateStatistics(
					dataStoreCli.getResult().createStore(),
					indexStoreCli.getResult().createStore(),
					statsStoreCli.getResult().createStore(),
					adapter,
					getAuthorizations(statsOperations.getAuthorizations())) ? 0 : -1);
		}
		catch (final ParseException e) {
			LOGGER.error(
					"Unable to parse stats tool arguments",
					e);
		}
		catch (final IOException e) {
			LOGGER.error(
					"Unable to parse stats tool arguments",
					e);
		}

	}

}
