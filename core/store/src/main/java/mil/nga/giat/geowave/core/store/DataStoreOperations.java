package mil.nga.giat.geowave.core.store;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.Deleter;
import mil.nga.giat.geowave.core.store.base.Reader;
import mil.nga.giat.geowave.core.store.base.Writer;
import mil.nga.giat.geowave.core.store.entities.GeoWaveRow;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.aggregate.Aggregation;

public interface DataStoreOperations
{

	public boolean indexExists(
			final ByteArrayId indexId )
			throws IOException;

	public void deleteAll()
			throws Exception;

	public boolean deleteAll(
			final ByteArrayId tableName,
			final ByteArrayId adapterId,
			final String... additionalAuthorizations );

	public boolean insureAuthorizations(
			final String clientUser,
			final String... authorizations );

	/**
	 * Creates a new writer that can be used by an index.
	 *
	 * @param indexId
	 *            The basic name of the table. Note that that basic
	 *            implementation of the factory will allow for a table namespace
	 *            to prefix this name
	 * @param adapterId
	 *            The name of the adapter.
	 * @param options
	 *            basic options available
	 * @param splits
	 *            If the table is created, these splits will be added as
	 *            partition keys. Null can be used to imply not to add any
	 *            splits.
	 * @return The appropriate writer
	 * @throws TableNotFoundException
	 *             The table does not exist in this Accumulo instance
	 */
	public Writer createWriter(
			final ByteArrayId indexId,
			final ByteArrayId adapterId,
			DataStoreOptions options,
			final Set<ByteArrayId> splits );

	public Reader createReader(
			PrimaryIndex index,
			List<ByteArrayId> adapterIds,
			double[] maxResolutionSubsamplingPerDimension,
			Pair<DataAdapter<?>, Aggregation<?, ?, ?>> aggregation,
			Pair<List<String>, DataAdapter<?>> fieldSubsets,
			boolean isWholeRow,
			QueryRanges ranges,
			DistributableQueryFilter filter,
			Integer limit,
			String... additionalAuthorizations );

	public Deleter<? extends GeoWaveRow> createDeleter(
			final ByteArrayId indexId,
			final String... authorizations )
			throws Exception;

}
