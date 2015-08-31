package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.PersistenceUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;

import org.apache.log4j.Logger;
import org.opengis.filter.Filter;

public class CQLQuery implements
		DistributableQuery
{
	private final static Logger LOGGER = Logger.getLogger(CQLQuery.class);
	private Query baseQuery;
	private CQLQueryFilter filter;

	protected CQLQuery() {}

	public CQLQuery(
			final Query baseQuery,
			final Filter filter,
			final FeatureDataAdapter adapter ) {
		this.baseQuery = baseQuery;
		this.filter = new CQLQueryFilter(
				filter,
				adapter);
	}

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final List<QueryFilter> queryFilters;
		if (baseQuery != null) {
			queryFilters = baseQuery.createFilters(indexModel);
		}
		else {
			queryFilters = new ArrayList<QueryFilter>();
		}
		if (filter != null) {
			queryFilters.add(filter);
		}
		return queryFilters;
	}

	@Override
	public boolean isSupported(
			final Index index ) {
		if (baseQuery != null) {
			return baseQuery.isSupported(index);
		}
		return true;
	}

	@Override
	public MultiDimensionalNumericData getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		if (baseQuery != null) {
			return baseQuery.getIndexConstraints(indexStrategy);
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		byte[] baseQueryBytes;
		if (baseQuery != null) {
			if (!(baseQuery instanceof DistributableQuery)) {
				throw new IllegalArgumentException(
						"Cannot distribute CQL query with base query of type '" + baseQuery.getClass() + "'");
			}
			else {
				baseQueryBytes = PersistenceUtils.toBinary((DistributableQuery) baseQuery);
			}
		}
		else {
			// base query can be null, no reason to log a warning
			baseQueryBytes = new byte[] {};
		}
		final byte[] filterBytes;
		if (filter != null) {
			filterBytes = filter.toBinary();
		}
		else {
			LOGGER.warn("Filter is null");
			filterBytes = new byte[] {};
		}

		final ByteBuffer buf = ByteBuffer.allocate(filterBytes.length + baseQueryBytes.length + 4);
		buf.putInt(filterBytes.length);
		buf.put(filterBytes);
		buf.put(baseQueryBytes);
		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final int filterBytesLength = buf.getInt();
		final int baseQueryBytesLength = bytes.length - filterBytesLength - 4;
		if (filterBytesLength > 0) {
			final byte[] filterBytes = new byte[filterBytesLength];

			filter = new CQLQueryFilter();
			filter.fromBinary(filterBytes);
		}
		else {
			LOGGER.warn("CQL filter is empty bytes");
			filter = null;
		}
		if (baseQueryBytesLength > 0) {
			final byte[] baseQueryBytes = new byte[baseQueryBytesLength];

			try {
				baseQuery = PersistenceUtils.fromBinary(
						baseQueryBytes,
						DistributableQuery.class);
			}
			catch (final Exception e) {
				throw new IllegalArgumentException(
						e);
			}
		}
		else {
			// base query can be null, no reason to log a warning
			baseQuery = null;
		}
	}
}
