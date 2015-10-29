package mil.nga.giat.geowave.core.store.index;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.QueryConstraints;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class CompositeConstraints implements
		FilterableConstraints
{
	private List<FilterableConstraints> constraints;

	public CompositeConstraints(
			List<FilterableConstraints> constraints ) {
		super();
		this.constraints = constraints;
	}

	public List<FilterableConstraints> getConstraints() {
		return constraints;
	}

	@Override
	public int getDimensionCount() {
		return constraints == null ? 0 : constraints.size();
	}

	@Override
	public boolean isEmpty() {
		return constraints == null || constraints.isEmpty();
	}

	@Override
	public DistributableQueryFilter getFilter() {
		List<DistributableQueryFilter> filters = new ArrayList<DistributableQueryFilter>();
		for (QueryConstraints constraint : constraints) {
			if (constraint instanceof FilterableConstraints) filters.add(((FilterableConstraints) constraint).getFilter());
		}
		return new DistributableFilterList(
				filters);
	}

	@Override
	public ByteArrayId getFieldId() {
		return constraints.get(0).getFieldId();
	}

	@Override
	public FilterableConstraints intersect(
			FilterableConstraints constaints ) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FilterableConstraints union(
			FilterableConstraints constaints ) {
		// TODO Auto-generated method stub
		return null;
	}


}
