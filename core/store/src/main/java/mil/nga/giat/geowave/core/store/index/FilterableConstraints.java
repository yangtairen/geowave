package mil.nga.giat.geowave.core.store.index;

import mil.nga.giat.geowave.core.index.QueryConstraints;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public interface FilterableConstraints extends
		QueryConstraints
{
	public DistributableQueryFilter getFilter();
}
