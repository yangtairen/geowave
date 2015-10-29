package mil.nga.giat.geowave.adapter.vector.query.cql;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;

public class PropertyConstraintSet
{
	private Map<ByteArrayId, FilterableConstraints> constraints = new HashMap<ByteArrayId, FilterableConstraints>();

	public PropertyConstraintSet() {}

	public PropertyConstraintSet(
			FilterableConstraints constraint ) {
		add(
				constraint,
				true);
	}

	public void add(
			FilterableConstraints constraint,
			boolean intersect ) {
		final ByteArrayId id = constraint.getFieldId();
		FilterableConstraints constraintsForId = constraints.get(id);
		if (constraintsForId == null) {
			constraints.put(
					id,
					constraint);
		}
		else if (intersect)
			constraints.put(
					id,
					constraintsForId.intersect(constraint));

		else
			constraints.put(
					id,
					constraintsForId.union(constraint));
	}

	public void intersect(
			PropertyConstraintSet set ) {
		for (Map.Entry<ByteArrayId, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					true);
		}
	}

	public void union(
			PropertyConstraintSet set ) {
		for (Map.Entry<ByteArrayId, FilterableConstraints> entry : set.constraints.entrySet()) {
			add(
					entry.getValue(),
					false);
		}
	}

	public FilterableConstraints getConstraintsById(
			ByteArrayId id ) {
		return constraints.get(id);
	}

}
