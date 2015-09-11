package mil.nga.giat.geowave.cli.stats;

import java.io.IOException;

import mil.nga.giat.geowave.core.cli.AdapterStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.CLIOperationDriver;
import mil.nga.giat.geowave.core.cli.CommandLineResult;
import mil.nga.giat.geowave.core.cli.DataStatisticsStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.DataStoreCommandLineOptions;
import mil.nga.giat.geowave.core.cli.IndexStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.AdapterStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.Query;

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
public class StatsOperation implements
		CLIOperationDriver
{
	private static final Logger LOGGER = Logger.getLogger(StatsOperationCLIProvider.class);

	public static boolean calculateStastics(
			final DataStore dataStore,
			final IndexStore indexStore,
			final AdapterStore adapterStore,
			final DataStatisticsStore statsStore,
			final ByteArrayId adapterId,
			final String[] authorizations )
			throws IOException {
		final DataAdapter<?> adapter = adapterStore.getAdapter(adapterId);
		if (adapter == null) {
			LOGGER.error("Unknown adapter " + adapterId);
			return false;
		}
		statsStore.removeAllStatistics(
				adapter.getAdapterId(),
				authorizations);
		try (StatsCompositionTool<?> statsTool = new StatsCompositionTool(
				adapter,
				statsStore)) {
			try (CloseableIterator<Index> indexit = indexStore.getIndices()) {
				while (indexit.hasNext()) {
					final Index index = indexit.next();
					try (CloseableIterator<?> entryIt = dataStore.query(
							adapter,
							index,
							(Query) null,
							(Integer) null,
							statsTool,
							authorizations)) {
						while (entryIt.hasNext()) {
							entryIt.next();
						}
					}
				}
			}
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Error while writing statistics.",
					ex);
			return false;
		}
		return true;
	}

	public static void main(
			final String args[] ) {
		final Options allOptions = new Options();
		DataStoreCommandLineOptions.applyOptions(allOptions);
		AdapterStoreCommandLineOptions.applyOptions(allOptions);
		IndexStoreCommandLineOptions.applyOptions(allOptions);
		DataStatisticsStoreCommandLineOptions.applyOptions(allOptions);
		StatsCommandLineOptions.applyOptions(allOptions);
		final BasicParser parser = new BasicParser();
		try {
			final CommandLine commandLine = parser.parse(
					allOptions,
					args,
					true);
			final boolean newCommandLine = true;
			while (newCommandLine) {
				final CommandLineResult<DataStoreCommandLineOptions> dataStoreCli = DataStoreCommandLineOptions.parseOptions(
						allOptions,
						commandLine);
				final CommandLineResult<AdapterStoreCommandLineOptions> adapterStoreCli = AdapterStoreCommandLineOptions.parseOptions(
						allOptions,
						commandLine);
				final IndexStoreCommandLineOptions indexStoreCli = IndexStoreCommandLineOptions.parseOptions(
						allOptions,
						commandLine);
				final DataStatisticsStoreCommandLineOptions statsStoreCli = DataStatisticsStoreCommandLineOptions.parseOptions(
						allOptions,
						commandLine);
				final StatsCommandLineOptions statsOperations = StatsCommandLineOptions.parseOptions(
						allOptions,
						commandLine);
			}
			System.exit(calculateStastics(
					dataStoreCli.createStore(),
					indexStoreCli.createStore(),
					adapterStoreCli.createStore(),
					statsStoreCli.createStore(),
					new ByteArrayId(
							statsOperations.getTypeName()),
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
		main(args);
	}

}
