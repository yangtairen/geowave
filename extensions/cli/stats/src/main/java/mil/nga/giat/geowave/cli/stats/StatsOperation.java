package mil.nga.giat.geowave.cli.stats;

import java.io.IOException;

import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatisticsStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.StatsCompositionTool;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.index.IndexStore;
import mil.nga.giat.geowave.core.store.query.Query;

import org.apache.log4j.Logger;

/**
 *
 * Simple command line tool to recalculate statistics for an adapter.
 *
 */
public class StatsOperation extends
		AbstractStatsOperation
{
	private static final Logger LOGGER = Logger.getLogger(StatsOperation.class);

	@Override
	protected boolean calculateStatistics(
			final DataStore dataStore,
			final IndexStore indexStore,
			final DataStatisticsStore statsStore,
			final DataAdapter<?> adapter,
			final String[] authorizations )
			throws IOException {
		statsStore.removeAllStatistics(
				adapter.getAdapterId(),
				authorizations);

		try (CloseableIterator<Index> indexit = indexStore.getIndices()) {
			while (indexit.hasNext()) {
				final Index index = indexit.next();
				statsStore.deleteObjects(
						index.getId(),
						authorizations);
				try (StatsCompositionTool<?> statsTool = new StatsCompositionTool(
						new DataAdapterStatsWrapper(
								index,
								adapter),
						statsStore)) {
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
}
