package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.util.List;

import mil.nga.giat.geowave.core.index.NumericIndexStrategy;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.Query;

public class CQLQuery implements
		DistributableQuery
{
	private Query baseQuery;
	private String cql;

	@Override
	public List<QueryFilter> createFilters(
			final CommonIndexModel indexModel ) {
		final List<QueryFilter> queryFilters = baseQuery.createFilters(indexModel);
		queryFilters.add(new CQLQueryFilter());
		return queryFilters;
	}

	@Override
	public boolean isSupported(
			final Index index ) {
		return false;
	}

	@Override
	public MultiDimensionalNumericData getIndexConstraints(
			final NumericIndexStrategy indexStrategy ) {
		return null;
	}

	@Override
	public byte[] toBinary() {
		return null;
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {

	}

}
