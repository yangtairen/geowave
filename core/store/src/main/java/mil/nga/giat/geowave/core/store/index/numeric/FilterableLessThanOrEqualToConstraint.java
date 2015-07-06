package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class FilterableLessThanOrEqualToConstraint extends
		NumericQueryConstraint
{

	public FilterableLessThanOrEqualToConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super(
				fieldId,
				number);
	}

	@Override
	public DistributableQueryFilter getFilter() {
		return new LessThanOrEqualToFilter(
				fieldId,
				number);
	}

	@Override
	public List<ByteArrayRange> getRange() {
		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						NumericIndexStrategy.toIndexByte(Double.MIN_VALUE)),
				new ByteArrayId(
						NumericIndexStrategy.toIndexByte(number.doubleValue()))));
	}

}
