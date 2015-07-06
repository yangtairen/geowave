package mil.nga.giat.geowave.core.store.index.numeric;

import java.util.Collections;
import java.util.List;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;

public class FilterableEqualsConstraint extends
		NumericQueryConstraint
{

	public FilterableEqualsConstraint(
			final ByteArrayId fieldId,
			final Number number ) {
		super(
				fieldId,
				number);
	}

	@Override
	public DistributableQueryFilter getFilter() {
		return new EqualsFilter(
				fieldId,
				number);
	}

	@Override
	public List<ByteArrayRange> getRange() {
		return Collections.singletonList(new ByteArrayRange(
				new ByteArrayId(
						NumericIndexStrategy.toIndexByte(number.doubleValue())),
				new ByteArrayId(
						NumericIndexStrategy.toIndexByte(number.doubleValue()))));
	}

}
