package mil.nga.giat.geowave.core.store.base.query;

import java.util.Collections;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.InsertionIds;
import mil.nga.giat.geowave.core.index.QueryRanges;
import mil.nga.giat.geowave.core.store.adapter.DataAdapter;
import mil.nga.giat.geowave.core.store.base.BaseDataStore;
import mil.nga.giat.geowave.core.store.callback.ScanCallback;
import mil.nga.giat.geowave.core.store.filter.DedupeFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.Query;

/**
 * Represents a query operation for a specific set of row IDs.
 *
 */
public class BaseRowIdsQuery<T> extends
		BaseConstraintsQuery
{
	final InsertionIds rows;

	public BaseRowIdsQuery(
			final BaseDataStore dataStore,
			final DataAdapter<T> adapter,
			final PrimaryIndex index,
			final InsertionIds rows,
			final ScanCallback<T, ?> scanCallback,
			final DedupeFilter dedupFilter,
			final String[] authorizations ) {
		super(
				dataStore,
				Collections.<ByteArrayId> singletonList(
						adapter.getAdapterId()),
				index,
				(Query) null,
				dedupFilter,
				scanCallback,
				null,
				null,
				null,
				null,
				null,
				authorizations);
		this.rows = rows;
	}

	@Override
	protected QueryRanges getRanges() {
		return rows.asQueryRanges();
	}
}