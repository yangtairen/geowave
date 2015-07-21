package mil.nga.giat.geowave.core.store.adapter.statistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.utils.DataStoreUtils;

/**
 * This is responsible for persisting data statistics (either in memory or to
 * disk depending on the implementation).
 */
public class MemoryDataStatisticsStore implements
		DataStatisticsStore
{
	Map<Key, DataStatistics<?>> statsMap;

	/**
	 * This will write the statistics to the underlying store. Note that this
	 * will overwrite whatever the current persisted statistics are with the
	 * given statistics ID and data adapter ID. Use incorporateStatistics to
	 * aggregate the statistics with any existing statistics.
	 * 
	 * @param statistics
	 *            The statistics to write
	 * 
	 */
	public void setStatistics(
			DataStatistics<?> statistics ) {
		statsMap.put(
				new Key(
						statistics.getDataAdapterId(),
						statistics.getStatisticsId(),
						statistics.getVisibility()),
				statistics);
	}

	/**
	 * Add the statistics to the store, overwriting existing data statistics
	 * with the aggregation of these statistics and the existing statistics
	 * 
	 * @param statistics
	 *            the data statistics
	 */
	public void incorporateStatistics(
			DataStatistics<?> statistics ) {
		final Key key = new Key(
				statistics.getDataAdapterId(),
				statistics.getStatisticsId(),
				statistics.getVisibility());
		DataStatistics<?> existingStats = statsMap.get(key);
		if (existingStats == null) {
			statsMap.put(
					key,
					statistics);
		}
		else {
			existingStats = PersistenceUtils.fromBinary(
					PersistenceUtils.toBinary(existingStats),
					DataStatistics.class);
			existingStats.merge(statistics);
			statsMap.put(
					key,
					statistics);
		}
	}

	/**
	 * Get all data statistics from the store by a data adapter ID
	 * 
	 * @param adapterId
	 *            the data adapter ID
	 * @return the list of statistics for the given adapter, empty if it doesn't
	 *         exist
	 */
	public CloseableIterator<DataStatistics<?>> getDataStatistics(
			ByteArrayId adapterId,
			String... authorizations ) {
		List<DataStatistics<?>> statSet = new ArrayList<DataStatistics<?>>();
		for (DataStatistics<?> stat : statsMap.values()) {
			if (stat.getDataAdapterId().equals(
					adapterId)) statSet.add(stat);

		}
		return new CloseableIterator.Wrapper<DataStatistics<?>>(
				statSet.iterator());
	}

	/**
	 * Get all data statistics from the store
	 * 
	 * @return the list of all statistics
	 */
	public CloseableIterator<DataStatistics<?>> getAllDataStatistics(
			String... authorizations ) {
		return new CloseableIterator.Wrapper<DataStatistics<?>>(
				statsMap.values().iterator());
	}

	/**
	 * Get statistics by adapter ID and the statistics ID (which will define a
	 * unique statistic)
	 * 
	 * @param adapterId
	 *            The adapter ID for the requested statistics
	 * @param statisticsId
	 *            the statistics ID for the requested statistics
	 * @return the persisted statistics value
	 */
	public DataStatistics<?> getDataStatistics(
			ByteArrayId adapterId,
			ByteArrayId statisticsId,
			String... authorizations ) {

		List<DataStatistics<?>> statSet = new ArrayList<DataStatistics<?>>();
		for (DataStatistics<?> stat : statsMap.values()) {
			if (stat.getDataAdapterId().equals(
					adapterId) && stat.getStatisticsId().equals(
					statisticsId) && DataStoreUtils.isAuthorized(
					stat.getVisibility(),
					authorizations)) statSet.add(stat);

		}

		return (statSet.size()) > 0 ? statSet.get(0) : null;
	}

	/**
	 * Remove a statistic from the store
	 * 
	 * @param adapterId
	 * @param statisticsId
	 * @return a flag indicating whether a statistic had existed with the given
	 *         IDs and was successfully deleted.
	 */
	public boolean removeStatistics(
			ByteArrayId adapterId,
			ByteArrayId statisticsId,
			String... authorizations ) {
		List<DataStatistics<?>> statSet = new ArrayList<DataStatistics<?>>();
		for (DataStatistics<?> stat : statsMap.values()) {
			if (stat.getDataAdapterId().equals(
					adapterId) && stat.getStatisticsId().equals(
					statisticsId) && DataStoreUtils.isAuthorized(
					stat.getVisibility(),
					authorizations)) statSet.add(stat);

		}
		if (statSet.size() > 0) {
			DataStatistics<?> statistics = statSet.get(0);
			statsMap.remove(new Key(
					statistics.getDataAdapterId(),
					statistics.getStatisticsId(),
					statistics.getVisibility()));
			return true;
		}
		return false;

	}

	private static class Key
	{
		ByteArrayId adapterId;
		ByteArrayId statisticsId;
		byte[] authorizations;

		public Key(
				ByteArrayId adapterId,
				ByteArrayId statisticsId,
				byte[] authorizations ) {
			super();
			this.adapterId = adapterId;
			this.statisticsId = statisticsId;
			this.authorizations = authorizations;
		}

		public ByteArrayId getAdapterId() {
			return adapterId;
		}

		public void setAdapterId(
				ByteArrayId adapterId ) {
			this.adapterId = adapterId;
		}

		public ByteArrayId getStatisticsId() {
			return statisticsId;
		}

		public void setStatisticsId(
				ByteArrayId statisticsId ) {
			this.statisticsId = statisticsId;
		}

		public byte[] getAuthorizations() {
			return authorizations;
		}

		public void setAuthorizations(
				byte[] authorizations ) {
			this.authorizations = authorizations;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((adapterId == null) ? 0 : adapterId.hashCode());
			result = prime * result + Arrays.hashCode(authorizations);
			result = prime * result + ((statisticsId == null) ? 0 : statisticsId.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (this == obj) return true;
			if (obj == null) return false;
			if (getClass() != obj.getClass()) return false;
			Key other = (Key) obj;
			if (adapterId == null) {
				if (other.adapterId != null) return false;
			}
			else if (!adapterId.equals(other.adapterId)) return false;
			if (!Arrays.equals(
					authorizations,
					other.authorizations)) return false;
			if (statisticsId == null) {
				if (other.statisticsId != null) return false;
			}
			else if (!statisticsId.equals(other.statisticsId)) return false;
			return true;
		}

	}
}